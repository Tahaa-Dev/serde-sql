// Cannot use `.inspect()` since it returns immutable references
// Have to use a manual `.inspect()` implementation that returns mutable references
#![allow(clippy::manual_inspect)]

use crate::{
    ColMap, Error, ErrorKind, FkAction, ForeignKey, GistBufMode, IndexMethod,
    IndexNullOrder, IndexSortOrder, IntervalField, ParserExt, PrimaryKey,
    Result, SqlColumn, SqlIndexColumn, SqlType, StrExt, SupportedDBs,
};

use nom::{
    IResult, Offset, Parser,
    branch::alt,
    bytes::complete::{is_not, tag, tag_no_case, take_until},
    character::complete::{
        alphanumeric1, anychar, char, digit1, multispace0, multispace1, none_of,
    },
    combinator::{map_res, not, opt, peek, recognize, value},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, preceded, separated_pair, terminated},
};

pub(crate) struct Lexer<'a> {
    #[allow(dead_code)]
    pub(crate) db: SupportedDBs,
    pub(crate) statements: &'a str,
    pub(crate) fks: Vec<(
        &'a str,         // referenced table
        Option<&'a str>, // referenced column
    )>,
    pub(crate) orig: &'a str, // original statements for error reporting
}

impl<'a> Lexer<'a> {
    pub(crate) fn parse_statement(&mut self) -> Result<Created<'_>> {
        // Self::parse_comment0 cannot fail
        self.parser(Self::parse_comment0)
            .map_into(ErrorKind::UnexpectedEOF, self.start_offset())?;

        let create = self.parser(tag_no_case("CREATE")).map_into(
            ErrorKind::InvalidCommand(self.next_token().to_string()),
            self.start_offset(),
        )?;
        self.parser(Self::parse_comment1).map_into(
            ErrorKind::NonWhitespace(create.to_string()),
            self.start_offset(),
        )?;

        let output = self
            .parser(alt((
                tag_no_case("TABLE"),
                recognize((
                    opt((tag_no_case("UNIQUE"), Self::parse_comment1)),
                    tag_no_case("INDEX"),
                )),
            )))
            .map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "TABLE/[UNIQUE] INDEX".to_string(),
                },
                self.start_offset(),
            )?;

        let output = output.to_uppercase();

        match output.as_str() {
            "TABLE" => self.pg_parse_table(),
            _ => self.pg_parse_index(output.starts_with("UNIQUE")),
        }
    }

    fn pg_parse_table(&mut self) -> Result<Created<'_>> {
        let mut columns = ColMap::new();
        let mut primary_key: Option<Pk> = None;
        let mut fks = vec![];

        self.parser(Self::parse_comment1).map_into(
            ErrorKind::NonWhitespace(self.next_token().to_string()),
            self.start_offset(),
        )?;

        let if_not_exists = self.parse_if_not_exists()?;

        let table_name = self.parser(Self::parse_ident).map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "table name".to_string(),
            },
            self.start_offset(),
        )?;
        self.parser(Self::parse_comment0).map_into(
            ErrorKind::NonWhitespace(self.next_token().to_string()),
            self.start_offset(),
        )?;

        // Has to be an inline closure for capturing `primary_key` and `columns` mutably
        let parse_col_def = |input| {
            if peek(tag_no_case::<&str, &str, nom::error::Error<&str>>(
                "FOREIGN",
            ))
            .parse(input)
            .is_ok()
            {
                let (input, fk_map) =
                    Self::pg_parse_table_level_fk(input, &mut fks)?;

                for (fk, fk_col) in fk_map {
                    if let Some(col) = columns.get_mut(fk_col) {
                        col.foreign_key = Some(fk);
                    } else {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::NoneOf,
                        )));
                    }
                }

                Ok((input, ()))
            } else if peek(tag_no_case::<&str, &str, nom::error::Error<&str>>(
                "PRIMARY",
            ))
            .parse(input)
            .is_ok()
            {
                let (input, pks) = preceded(
                    (
                        tag_no_case("PRIMARY"),
                        Self::parse_comment1,
                        tag_no_case("KEY"),
                        Self::parse_comment0,
                    ),
                    Self::parse_list(Self::parse_ident),
                )
                .parse(input)?;

                primary_key = Some(Pk::Composite(pks));

                Ok((input, ()))
            } else {
                let (input, col_name) = Self::parse_ident(input)?;
                let (input, _) = Self::parse_comment1(input)?;

                let (input, (sql_type, args)) = Self::pg_parse_type(input)?;
                let (input, arr) =
                    opt(preceded(Self::parse_comment0, many1(tag("[]"))))
                        .parse(input)?;
                let args = args.unwrap_or(Vec::new());
                let ty = sql_type.to_uppercase();

                let sql_type = Self::match_type(ty, args.as_slice());

                let mut is_primary_key = false;
                let mut not_null = false;
                let mut index = None;
                let mut default = None;
                let mut check = None;
                let mut foreign_key: Option<ForeignKey> = None;

                let (input, pk) =
                    many0(Self::pg_parse_constraint).parse(input)?;

                for s in pk {
                    match s {
                        Constraint::PrimaryKey => {
                            if primary_key.is_some() {
                                return Err(nom::Err::Failure(
                                    nom::error::Error::new(
                                        input,
                                        nom::error::ErrorKind::IsNot,
                                    ),
                                ));
                            } else {
                                is_primary_key = true;
                                not_null = true;
                                index = Some(SqlIndexColumn::default());
                                primary_key = Some(Pk::Single(col_name));
                            }
                        }

                        Constraint::Unique => {
                            index = Some(SqlIndexColumn::default());
                        }
                        Constraint::NotNull => not_null = true,

                        Constraint::Def(expr) => {
                            default = Some(expr.to_string());
                        }
                        Constraint::Check(expr) => {
                            check = Some(expr[1..expr.len() - 1].to_string());
                        }
                        Constraint::FkAction { event, action } => {
                            if let Some(ref mut foreign_key) = foreign_key {
                                match event {
                                    OnEvent::Delete
                                        if foreign_key.on_delete.is_none() =>
                                    {
                                        foreign_key.on_delete = Some(action);
                                    }
                                    OnEvent::Update
                                        if foreign_key.on_update.is_none() =>
                                    {
                                        foreign_key.on_update = Some(action);
                                    }
                                    _ => {
                                        return Err(nom::Err::Failure(
                                            nom::error::Error::new(
                                                "ON EVENT",
                                                nom::error::ErrorKind::IsNot,
                                            ),
                                        ));
                                    }
                                }
                            }
                        }

                        Constraint::ForeignKey { table, col } => {
                            fks.push((table, col));
                            foreign_key = Some(ForeignKey {
                                table: table.to_string(),
                                column: col.map(String::from),
                                on_delete: None,
                                on_update: None,
                            });
                        }
                    }
                }

                let sql_type = if let Some(arr) = arr {
                    SqlType::Array(Box::new(sql_type), arr.len())
                } else {
                    sql_type
                };

                let opt = columns.insert(
                    col_name.to_string(),
                    SqlColumn {
                        sql_type,
                        index,
                        not_null,
                        is_primary_key,
                        default,
                        check,
                        foreign_key,
                    },
                );

                if opt.is_some() {
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        col_name,
                        nom::error::ErrorKind::OneOf,
                    )));
                }

                Ok((input, ()))
            }
        };

        self.parser(Self::parse_list(parse_col_def)).map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "column definitions".to_string(),
            },
            self.start_offset(),
        )?;

        self.fks.extend_from_slice(&fks);

        Ok(Created::Table {
            name: table_name,
            columns,
            primary_key,
            if_not_exists,
        })
    }

    fn pg_parse_table_level_fk(
        input: &'a str,
        fks: &mut Vec<(&'a str, Option<&'a str>)>,
    ) -> IResult<&'a str, impl Iterator<Item = (ForeignKey, &'a str)>> {
        let (input, _) = Self::parse_comment0(input)?;

        let (input, _) = tag_no_case("FOREIGN")(input)?;
        let (input, _) = Self::parse_comment1(input)?;
        let (input, _) = tag_no_case("KEY")(input)?;
        let (input, _) = Self::parse_comment0(input)?;

        let (input, fk_cols): (&'a str, Vec<&'a str>) =
            Self::parse_list(Self::parse_ident).parse(input)?;

        let (input, _) = Self::parse_comment1(input)?;
        let (input, _) = tag_no_case("REFERENCES")(input)?;
        let (input, _) = Self::parse_comment1(input)?;

        let (input, ref_table) = Self::parse_ident(input)?;
        let (input, _) = Self::parse_comment0(input)?;

        let (input, ref_cols) =
            opt(Self::parse_list(Self::parse_ident)).parse(input)?;

        let mut on_delete = None;
        let mut on_update = None;

        let (input, action) =
            opt(preceded(Self::parse_comment0, Self::pg_parse_fkaction))
                .parse(input)?;
        if let Some(action) = action
            && let Constraint::FkAction { event, action } = action
        {
            match event {
                OnEvent::Delete => on_delete = Some(action),
                OnEvent::Update => on_update = Some(action),
            }
        }

        let (input, action) =
            opt(preceded(Self::parse_comment0, Self::pg_parse_fkaction))
                .parse(input)?;
        if let Some(action) = action
            && let Constraint::FkAction { event, action } = action
        {
            match event {
                OnEvent::Delete => on_delete = Some(action),
                OnEvent::Update => on_update = Some(action),
            }
        }

        Ok((
            input,
            fk_cols.into_iter().enumerate().map(move |(i, fk_col)| {
                let ref_col =
                    ref_cols.as_ref().and_then(|cols| cols.get(i).copied());
                fks.push((ref_table, ref_col));

                (
                    ForeignKey {
                        table: ref_table.to_string(),
                        column: ref_col.map(String::from),
                        on_delete,
                        on_update,
                    },
                    fk_col,
                )
            }),
        ))
    }

    fn pg_parse_type(
        input: &'a str,
    ) -> IResult<&'a str, (&'a str, Option<Vec<usize>>)> {
        let (input, sql_type) = alt((
            recognize((
                tag_no_case("DOUBLE"),
                multispace1,
                tag_no_case("PRECISION"),
            )),
            recognize((
                tag_no_case("TIMESTAMP"),
                multispace1,
                tag_no_case("WITH"),
                multispace1,
                tag_no_case("TIME"),
                multispace1,
                tag_no_case("ZONE"),
            )),
            recognize((
                tag_no_case("TIMESTAMP"),
                multispace1,
                tag_no_case("WITHOUT"),
                multispace1,
                tag_no_case("TIME"),
                multispace1,
                tag_no_case("ZONE"),
            )),
            recognize((
                tag_no_case("CHARACTER"),
                multispace1,
                tag_no_case("VARYING"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("YEAR"),
                multispace1,
                tag_no_case("TO"),
                multispace1,
                tag_no_case("MONTH"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("DAY"),
                multispace1,
                tag_no_case("TO"),
                multispace1,
                tag_no_case("HOUR"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("DAY"),
                multispace1,
                tag_no_case("TO"),
                multispace1,
                tag_no_case("MINUTE"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("DAY"),
                multispace1,
                tag_no_case("TO"),
                multispace1,
                tag_no_case("SECOND"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("HOUR"),
                multispace1,
                tag_no_case("TO"),
                multispace1,
                tag_no_case("MINUTE"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("HOUR"),
                multispace1,
                tag_no_case("TO"),
                multispace1,
                tag_no_case("SECOND"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("MINUTE"),
                multispace1,
                tag_no_case("TO"),
                multispace1,
                tag_no_case("SECOND"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("YEAR"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("MONTH"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("DAY"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("HOUR"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("MINUTE"),
            )),
            recognize((
                tag_no_case("INTERVAL"),
                multispace1,
                tag_no_case("SECOND"),
            )),
            alphanumeric1,
        ))
        .parse(input)?;

        let (input, args) = opt(preceded(
            Self::parse_comment0,
            Self::parse_list(map_res(digit1, |n: &'a str| n.parse::<usize>())),
        ))
        .parse(input)?;

        Ok((input, (sql_type, args)))
    }

    fn match_type(ty: String, args: &[usize]) -> SqlType {
        match ty.as_str() {
            "SMALLINT" | "INT2" => SqlType::SmallInt,
            "INTEGER" | "INT" | "INT4" => SqlType::Integer,
            "BIGINT" | "INT8" => SqlType::BigInt,
            "REAL" | "FLOAT4" => SqlType::Real,
            "DOUBLE PRECISION" | "FLOAT8" => SqlType::DoublePrecision,
            "DECIMAL" => {
                SqlType::Decimal(args.first().copied(), args.get(1).copied())
            }
            "NUMERIC" => {
                SqlType::Numeric(args.first().copied(), args.get(1).copied())
            }
            "CHAR" | "CHARACTER" => {
                SqlType::Char(args.first().copied().unwrap_or(1))
            }

            "VARCHAR" | "CHARACTER VARYING" => {
                SqlType::VarChar(args.first().copied())
            }

            "TEXT" => SqlType::Text,
            "BYTEA" => SqlType::ByteA,
            "BOOLEAN" | "BOOL" => SqlType::Boolean,
            "INET" => SqlType::Inet,
            "CIDR" => SqlType::Cidr,
            "MACADDR" => SqlType::MacAddr,
            "JSON" => SqlType::Json,
            "JSONB" => SqlType::Jsonb,
            "UUID" => SqlType::Uuid,
            "SMALLSERIAL" | "SERIAL2" => SqlType::SmallSerial,
            "SERIAL" | "SERIAL4" => SqlType::Serial,
            "BIGSERIAL" | "SERIAL8" => SqlType::BigSerial,
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
                SqlType::Timestamp(args.first().copied())
            }

            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
                SqlType::Timestamptz(args.first().copied())
            }

            "DATE" => SqlType::Date,
            "TIME" => SqlType::Time(args.first().copied()),

            _ if ty.starts_with("INTERVAL") => SqlType::Interval {
                fields: match ty
                    .split_whitespace()
                    .collect::<Vec<&str>>()
                    .as_slice()
                {
                    ["INTERVAL", "YEAR"] => IntervalField::Year,
                    ["INTERVAL", "MONTH"] => IntervalField::Month,
                    ["INTERVAL", "DAY"] => IntervalField::Day,
                    ["INTERVAL", "HOUR"] => IntervalField::Hour,
                    ["INTERVAL", "MINUTE"] => IntervalField::Minute,
                    ["INTERVAL", "SECOND"] => IntervalField::Second,
                    ["INTERVAL", "YEAR", "TO", "MONTH"] => {
                        IntervalField::YearToMonth
                    }
                    ["INTERVAL", "DAY", "TO", "HOUR"] => {
                        IntervalField::DayToHour
                    }
                    ["INTERVAL", "DAY", "TO", "MINUTE"] => {
                        IntervalField::DayToMinute
                    }
                    ["INTERVAL", "DAY", "TO", "SECOND"] => {
                        IntervalField::DayToSecond
                    }
                    ["INTERVAL", "HOUR", "TO", "MINUTE"] => {
                        IntervalField::HourToMinute
                    }
                    ["INTERVAL", "HOUR", "TO", "SECOND"] => {
                        IntervalField::HourToSecond
                    }
                    ["INTERVAL", "MINUTE", "TO", "SECOND"] => {
                        IntervalField::MinuteToSecond
                    }
                    ["INTERVAL"] => IntervalField::None,
                    _ => unreachable!(),
                },
                precision: args.first().copied(),
            },

            _ => SqlType::Unknown(ty),
        }
    }

    fn pg_parse_constraint(input: &'a str) -> IResult<&'a str, Constraint<'a>> {
        preceded(
            Self::parse_comment1,
            alt((
                value(
                    Constraint::PrimaryKey,
                    (tag_no_case("PRIMARY"), multispace1, tag_no_case("KEY")),
                ),
                value(Constraint::Unique, tag_no_case("UNIQUE")),
                value(
                    Constraint::NotNull,
                    (tag_no_case("NOT"), multispace1, tag_no_case("NULL")),
                ),
                preceded(
                    (tag_no_case("CHECK"), Self::parse_comment0),
                    Self::parse_parens.map(Constraint::Check),
                ),
                Self::parse_default,
                Self::pg_parse_inline_fk,
                Self::pg_parse_fkaction,
            )),
        )
        .parse(input)
    }

    fn parse_default(input: &str) -> IResult<&str, Constraint<'_>> {
        preceded(
            (tag_no_case("DEFAULT"), Self::parse_comment1),
            recognize(alt((
                delimited(
                    tag("\""),
                    alt((
                        recognize(many0((is_not("\\"), tag("\\"), anychar))),
                        recognize(many0((is_not("\""), tag("\""), char('"')))),
                        is_not("\""),
                    )),
                    tag("\""),
                ),
                delimited(
                    tag("'"),
                    alt((
                        recognize(many0((is_not("\\"), tag("\\"), anychar))),
                        recognize(many0((is_not("\'"), tag("''")))),
                        is_not("'"),
                    )),
                    tag("'"),
                ),
                Self::parse_parens,
            )))
            .map(Constraint::Def),
        )
        .parse(input)
    }

    fn pg_parse_inline_fk(input: &str) -> IResult<&str, Constraint<'_>> {
        preceded(
            (tag_no_case("REFERENCES"), Self::parse_comment1),
            (
                Self::parse_ident,
                opt(delimited(
                    (tag("("), Self::parse_comment0),
                    Self::parse_ident,
                    (Self::parse_comment0, tag(")")),
                )),
            )
                .map(|(table, col)| Constraint::ForeignKey { table, col }),
        )
        .parse(input)
    }

    fn pg_parse_fkaction(input: &str) -> IResult<&str, Constraint<'a>> {
        let (input, _) = tag_no_case("ON")(input)?;
        let (input, _) = Self::parse_comment1(input)?;
        let (input, event) = alt((
            value(OnEvent::Delete, tag_no_case("DELETE")),
            value(OnEvent::Update, tag_no_case("UPDATE")),
        ))
        .parse(input)?;
        let (input, _) = Self::parse_comment1(input)?;
        let (input, action) = alt((
            value(FkAction::Restrict, tag_no_case("RESTRICT")),
            value(FkAction::Cascade, tag_no_case("CASCADE")),
            value(
                FkAction::NoAction,
                recognize((
                    tag_no_case("NO"),
                    multispace1,
                    tag_no_case("ACTION"),
                )),
            ),
            value(
                FkAction::SetNull,
                recognize((
                    tag_no_case("SET"),
                    multispace1,
                    tag_no_case("NULL"),
                )),
            ),
            value(
                FkAction::SetDefault,
                recognize((
                    tag_no_case("SET"),
                    multispace1,
                    tag_no_case("DEFAULT"),
                )),
            ),
        ))
        .parse(input)?;

        Ok((input, Constraint::FkAction { event, action }))
    }

    fn pg_parse_index(&mut self, is_unique: bool) -> Result<Created<'_>> {
        self.parser(Self::parse_comment1).map_into(
            ErrorKind::NonWhitespace(self.next_token().to_string()),
            self.start_offset(),
        )?;

        let is_concurrent = self
            .parser(opt(terminated(
                tag_no_case("CONCURRENTLY"),
                Self::parse_comment1,
            )))
            .map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "CONCURRENTLY".to_string(),
                },
                self.start_offset(),
            )?
            .is_some();

        self.parse_if_not_exists()?;

        let (index_name, table_name): (Option<&str>, &str) = self
            .parser(alt((
                (
                    opt(terminated(Self::parse_ident, Self::parse_comment1)),
                    preceded(
                        (tag_no_case("ON"), Self::parse_comment1),
                        Self::parse_ident,
                    ),
                ),
                (
                    opt(recognize(not(is_not("")))),
                    preceded(
                        (tag_no_case("ON"), Self::parse_comment1),
                        Self::parse_ident,
                    ),
                ),
            )))
            .map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "index_name ON table_name".to_string(),
                },
                self.start_offset(),
            )?;

        self.parser(Self::parse_comment1).map_into(
            ErrorKind::NonWhitespace(self.next_token().to_string()),
            self.start_offset(),
        )?;

        let (idx_method, mut index_method) =
            self.pg_parse_index_method().map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "index method (USING ...)".to_string(),
                },
                self.start_offset(),
            )?;

        self.parser(Self::parse_comment1).map_into(
            ErrorKind::NonWhitespace(self.next_token().to_string()),
            self.start_offset(),
        )?;

        let cols = self
            .parser(Self::parse_list(map_res(
                (
                    alt((
                        recognize((
                            many0(alt((alphanumeric1, tag("_")))),
                            delimited(
                                (
                                    Self::parse_comment0,
                                    tag("("),
                                    Self::parse_comment0,
                                ),
                                Self::parse_ident,
                                (Self::parse_comment0, tag("(")),
                            ),
                        )),
                        Self::parse_ident,
                    )),
                    opt(preceded(Self::parse_comment1, Self::parse_ident)),
                    opt(preceded(
                        Self::parse_comment1,
                        alt((
                            value(IndexSortOrder::Asc, tag_no_case("ASC")),
                            value(IndexSortOrder::Desc, tag_no_case("DESC")),
                        )),
                    )),
                    opt(preceded(
                        Self::parse_comment1,
                        alt((
                            value(
                                IndexNullOrder::NullsFirst,
                                (
                                    tag_no_case("NULLS"),
                                    Self::parse_comment1,
                                    tag_no_case("FIRST"),
                                ),
                            ),
                            value(
                                IndexNullOrder::NullsLast,
                                (
                                    tag_no_case("NULLS"),
                                    Self::parse_comment1,
                                    tag_no_case("LAST"),
                                ),
                            ),
                        )),
                    )),
                ),
                |(name, opclass, sort1, sort2)| {
                    Ok::<
                        (&str, SqlIndexColumn),
                        nom::Err<nom::error::Error<&str>>,
                    >((
                        name,
                        SqlIndexColumn {
                            name: index_name.map(String::from),
                            opclass: opclass.map(String::from),
                            sort_order: sort1,
                            null_order: sort2,
                            method: index_method,
                            is_concurrent,
                            is_unique,
                            included_cols: None,
                            predicate: None,
                        },
                    ))
                },
            )))
            .map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "index columns".to_string(),
                },
                self.start_offset(),
            )?;

        let included = self.pg_parse_include().map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "INCLUDE clause".to_string(),
            },
            self.start_offset(),
        )?;

        self.pg_apply_with_params(&mut index_method, idx_method)?;

        let predicate = self.pg_parse_where().map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "WHERE clause".to_string(),
            },
            self.start_offset(),
        )?;

        Ok(Created::Index { table_name, columns: cols, included, predicate })
    }

    fn pg_parse_index_method(
        &mut self,
    ) -> Result<(Option<&'a str>, Option<IndexMethod>)> {
        let idx_method = self
            .parser(opt(preceded(
                (tag_no_case("USING"), Self::parse_comment1),
                alphanumeric1,
            )))
            .map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "index method name".to_string(),
                },
                self.start_offset(),
            )?;

        let index_method =
            idx_method.map(|s: &str| match s.to_uppercase().as_str() {
                "BTREE" => IndexMethod::BTree { fillfactor: None },
                "HASH" => IndexMethod::Hash { fillfactor: None },
                "GIN" => IndexMethod::Gin {
                    fastupdate: None,
                    gin_pending_list_limit: None,
                },
                "GIST" => {
                    IndexMethod::Gist { fillfactor: None, buffering: None }
                }
                "BRIN" => IndexMethod::Brin {
                    pages_per_range: None,
                    autosummarize: None,
                },
                "SPGIST" => IndexMethod::SpGist { fillfactor: None },
                _ => IndexMethod::Other,
            });

        if let Some(IndexMethod::Other) = index_method {
            return Err(Error::new(
                ErrorKind::InvalidIndexMethod(
                    unsafe { idx_method.unwrap_unchecked() }.to_string(),
                ),
                self.start_offset(),
            ));
        }

        Ok((idx_method, index_method))
    }

    fn pg_parse_include(&mut self) -> Result<Option<Vec<&'a str>>> {
        self.parser(opt(preceded(
            (
                Self::parse_comment1,
                tag_no_case("INCLUDE"),
                Self::parse_comment0,
            ),
            Self::parse_list(Self::parse_ident),
        )))
        .map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "INCLUDE clause".to_string(),
            },
            self.start_offset(),
        )
    }

    fn pg_apply_with_params(
        &mut self,
        index_method: &mut Option<IndexMethod>,
        idx_method: Option<&str>,
    ) -> Result<()> {
        let pairs = self
            .parser(opt(preceded(
                (
                    Self::parse_comment1,
                    tag_no_case("WITH"),
                    Self::parse_comment0,
                ),
                Self::parse_list(separated_pair(
                    Self::parse_ident,
                    (Self::parse_comment0, tag("="), Self::parse_comment0),
                    alphanumeric1,
                )),
            )))
            .map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "WITH clause".to_string(),
                },
                self.start_offset(),
            )?;

        if pairs.is_some() {
            idx_method.ok_or(Error::new(
                ErrorKind::UnexpectedToken {
                    expected: self.statements.into(),
                    found: "INDEX METHOD".into(),
                },
                self.start_offset(),
            ))?;
        }

        for (key, value) in pairs.unwrap_or(vec![]) {
            let res = match key.to_lowercase().as_str() {
                "fillfactor" => {
                    let v = value.parse::<u8>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid fillfactor value".into(),
                            ),
                            self.start_offset(),
                        )
                    })?;
                    let mut r = Ok(());
                    index_method.as_mut().map(|m| {
                        r = m.set_fillfactor(v);
                        m
                    });
                    r
                }

                "fastupdate" => {
                    let v = value.parse::<bool>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid fastupdate value".into(),
                            ),
                            self.start_offset(),
                        )
                    })?;
                    let mut r = Ok(());
                    index_method.as_mut().map(|m| {
                        r = m.set_fastupdate(v);
                        m
                    });
                    r
                }

                "gin_pending_list_limit" => {
                    let v = value.parse::<u32>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid gin_pending_list_limit value".into(),
                            ),
                            self.start_offset(),
                        )
                    })?;
                    let mut r = Ok(());
                    index_method.as_mut().map(|m| {
                        r = m.set_gin_pending_list_limit(v);
                        m
                    });
                    r
                }

                "buffering" => {
                    let v = match value.to_uppercase().as_str() {
                        "ON" | "TRUE" => GistBufMode::On,
                        "OFF" | "FALSE" => GistBufMode::Off,
                        "AUTO" => GistBufMode::Auto,
                        _ => {
                            return Err(Error::new(
                                ErrorKind::InvalidValue(value.to_string()),
                                self.start_offset(),
                            ));
                        }
                    };

                    let mut r = Ok(());
                    index_method.as_mut().map(|m| {
                        r = m.set_buffering(v);
                        m
                    });
                    r
                }

                "autosummarize" => {
                    let v = match value.to_uppercase().as_str() {
                        "ON" | "TRUE" => true,
                        "OFF" | "FALSE" => false,
                        _ => {
                            return Err(Error::new(
                                ErrorKind::InvalidValue(value.to_string()),
                                self.start_offset(),
                            ));
                        }
                    };

                    let mut r = Ok(());
                    index_method.as_mut().map(|m| {
                        r = m.set_autosummarize(v);
                        m
                    });
                    r
                }

                "pages_per_range" => {
                    let v = value.parse::<u32>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid pages_per_range value".into(),
                            ),
                            self.start_offset(),
                        )
                    })?;
                    let mut r = Ok(());
                    index_method.as_mut().map(|m| {
                        r = m.set_pages_per_range(v);
                        m
                    });
                    r
                }

                _ => Err(()),
            };

            res.map_err(|_| {
                Error::new(
                    ErrorKind::InvalidStorageParam {
                        key: key.to_string(),
                        value: value.to_string(),
                    },
                    self.start_offset(),
                )
            })?;
        }

        Ok(())
    }

    fn pg_parse_where(&mut self) -> Result<Option<String>> {
        let out = self
            .parser(opt(preceded(
                (
                    Self::parse_comment1,
                    tag_no_case("WHERE"),
                    Self::parse_comment1,
                ),
                recognize(many1(alt((
                    recognize(delimited(
                        tag("'"),
                        many0(alt((
                            tag("\\'"),
                            tag("''"),
                            recognize(none_of("'")),
                        ))),
                        tag("'"),
                    )),
                    recognize(delimited(
                        tag("\""),
                        many0(alt((
                            tag("\\\""),
                            tag("\"\""),
                            recognize(none_of("\"")),
                        ))),
                        tag("\""),
                    )),
                    is_not(";'\""),
                )))),
            )))
            .map_into(
                ErrorKind::UnexpectedToken {
                    found: self.next_token().to_string(),
                    expected: "WHERE clause".to_string(),
                },
                self.start_offset(),
            )?;

        Ok(out.map(|s| s.trim().to_string()))
    }

    fn parse_if_not_exists(&mut self) -> Result<bool> {
        self.parser(opt((
            tag_no_case("IF"),
            Self::parse_comment1,
            tag_no_case("NOT"),
            Self::parse_comment1,
            tag_no_case("EXISTS"),
            Self::parse_comment1,
        )))
        .map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "IF NOT EXISTS".to_string(),
            },
            self.start_offset(),
        )
        .map(|is| is.is_some())
    }

    fn parse_list<
        T,
        P1: Parser<&'a str, Output = T, Error = nom::error::Error<&'a str>>,
    >(
        f: P1,
    ) -> impl Parser<&'a str, Output = Vec<T>, Error = nom::error::Error<&'a str>>
    {
        delimited(
            (tag("("), Self::parse_comment0),
            separated_list1(
                (Self::parse_comment0, tag(","), Self::parse_comment0),
                f,
            ),
            (Self::parse_comment0, tag(")")),
        )
    }

    fn parse_parens(input: &str) -> IResult<&str, &str> {
        let mut depth = 0;
        let mut chars = input.char_indices().peekable();
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut escaped = false;

        while let Some((i, c)) = chars.next() {
            match c {
                '\\' => escaped = !escaped,

                '\'' if !in_double_quote => {
                    if chars.peek().is_some_and(|(_, ch)| *ch != '\'')
                        && !escaped
                    {
                        in_single_quote = !in_single_quote;
                    }
                }

                '"' if !in_single_quote => {
                    if chars.peek().is_some_and(|(_, ch)| *ch != '"')
                        && !escaped
                    {
                        in_double_quote = !in_double_quote;
                    }
                }

                '(' if !in_single_quote && !in_double_quote => {
                    depth += 1;
                }

                ')' if !in_single_quote && !in_double_quote => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok((&input[i + 1..], &input[..i + 1]));
                    }
                }

                _ => escaped = false,
            }
        }

        // Reached EOF without finding closing paren
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TakeUntil,
        )))
    }

    pub(crate) fn parser<
        T,
        F: Parser<&'a str, Output = T, Error = nom::error::Error<&'a str>>,
    >(
        &mut self,
        mut parser: F,
    ) -> Result<T> {
        let (input, out) = match parser.parse(self.statements) {
            Ok(ok) => Ok(ok),
            Err(nom::Err::Error(_)) | Err(nom::Err::Failure(_)) => {
                let found = self.next_token().to_string();
                Err(Error::new(
                    ErrorKind::UnexpectedToken {
                        found,
                        expected: "valid SQL syntax".into(),
                    },
                    self.start_offset(),
                ))
            }
            Err(nom::Err::Incomplete(_)) => {
                Err(Error::new(ErrorKind::UnexpectedEOF, self.start_offset()))
            }
        }?;

        self.statements = input;

        Ok(out)
    }

    pub(crate) fn next_token(&self) -> &str {
        self.statements.next_token()
    }

    pub(crate) fn start_offset(&self) -> usize {
        self.orig.offset(self.statements) + 1
    }

    fn parse_ident(input: &str) -> IResult<&str, &str> {
        alt((recognize(many1(alt((tag("_"), alphanumeric1)))), |input| {
            let (input, _) = tag("\"")(input)?;
            let (input, output) = nom::bytes::complete::is_not("\"")(input)?;
            let (input, _) = tag("\"")(input)?;
            Ok((input, output))
        }))
        .parse(input)
    }

    pub(crate) fn parse_comment0(input: &str) -> IResult<&str, &str> {
        let (input, _) = multispace0(input)?;

        let (input, _) = many0(alt((
            // many0(none_of("\r\n")) instead of is_not("\r\n") is because is_not fails if the pattern
            // is not found while many0(none_of) doesn't which is needed since the comment could be at
            // EOF where is_not wouldn't find \n or \r and it would fail
            value((), (tag("--"), many0(none_of("\r\n")), multispace0)),
            value((), (tag("/*"), take_until("*/"), tag("*/"), multispace0)),
        )))
        .parse(input)?;

        Ok((input, ""))
    }

    pub(crate) fn parse_comment1(input: &str) -> IResult<&str, &str> {
        let (input, _) = multispace1(input)?;

        let (input, _) = many0(alt((
            value((), (tag("--"), many0(none_of("\r\n")), multispace1)),
            value((), (tag("/*"), take_until("*/"), tag("*/"), multispace1)),
        )))
        .parse(input)?;

        Ok((input, ""))
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Created<'a> {
    Table {
        name: &'a str,
        columns: ColMap,
        primary_key: Option<Pk<'a>>,
        if_not_exists: bool,
    },

    Index {
        table_name: &'a str,
        columns: Vec<(&'a str, SqlIndexColumn)>,
        included: Option<Vec<&'a str>>,
        predicate: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum Pk<'a> {
    Single(&'a str),
    Composite(Vec<&'a str>),
}
impl<'a> Pk<'a> {
    pub(crate) fn into_primary_key(self) -> PrimaryKey {
        match self {
            Self::Single(s) => PrimaryKey::Single(s.to_string()),
            Self::Composite(v) => PrimaryKey::Composite(
                v.into_iter().map(|s| s.to_string()).collect(),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum OnEvent {
    Delete,
    Update,
}

#[derive(Clone, Debug)]
enum Constraint<'a> {
    PrimaryKey,
    Unique,
    NotNull,
    Check(&'a str),
    Def(&'a str),
    ForeignKey { table: &'a str, col: Option<&'a str> },
    FkAction { event: OnEvent, action: FkAction },
}

#[cfg(test)]
mod tests {
    use crate::{SupportedDBs, lexer::Lexer};

    #[test]
    fn table_lexer_valid() {
        let statements = r#"CREATE table IF   NOT   EXISTS -- Make sure it's only created once
            users (
                id UUID primary   key, /* Primary key Notes */
                name TEXT,
                time INTERVAL    DAY TO  SECOND 
            )"#;

        let mut lexer = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements,
            fks: vec![],
            orig: statements,
        };

        lexer.parse_statement().unwrap();

        assert!(lexer.statements.is_empty());
    }

    #[test]
    fn index_lexer_valid() {
        let statements = r#"CREATE InDex -- Note
            IF NOT EXISTS user_id ON users USING BRIN (
                id, /* Multi-line
                    Note */
                name
            )"#;

        let mut lexer2 = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements,
            fks: vec![],
            orig: statements,
        };

        lexer2.parse_statement().unwrap();

        assert!(lexer2.statements.is_empty());
    }
}
