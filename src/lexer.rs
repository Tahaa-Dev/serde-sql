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
        alpha1, alphanumeric1, digit1, multispace0, multispace1, none_of,
    },
    combinator::{map_res, opt, peek, recognize, value},
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

type IdxCol<'a> =
    (&'a str, Option<&'a str>, Option<IndexSortOrder>, Option<IndexNullOrder>);

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

    pub(crate) fn pg_parse_table(&mut self) -> Result<Created<'_>> {
        let mut columns = ColMap::new();
        let mut primary_key: Option<Pk> = None;
        let mut fks = vec![];
        let mut check = None;

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
            } else if peek(tag_no_case::<&str, &str, nom::error::Error<&str>>(
                "UNIQUE",
            ))
            .parse(input)
            .is_ok()
            {
                let (input, cols) = preceded(
                    (tag_no_case("UNIQUE"), Self::parse_comment0),
                    Self::parse_list(Self::parse_ident),
                )
                .parse(input)?;

                for col in cols {
                    if let Some(col) = columns.get_mut(col) {
                        col.index = Some(SqlIndexColumn::default());
                    } else {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::NoneOf,
                        )));
                    }
                }

                Ok((input, ()))
            } else if peek(tag_no_case::<&str, &str, nom::error::Error<&str>>(
                "CHECK",
            ))
            .parse(input)
            .is_ok()
            {
                let (input, constraint) = Self::pg_parse_constraint(input)?;

                check = Some(match constraint {
                    Constraint::Check(s) => s[1..s.len() - 1].to_string(),
                    _ => unreachable!(),
                });

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

                let (input, pk) = many0(preceded(
                    Self::parse_comment1,
                    Self::pg_parse_constraint,
                ))
                .parse(input)?;

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
            check,
        })
    }

    pub(crate) fn pg_parse_table_level_fk(
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

    pub(crate) fn pg_parse_type(
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

    pub(crate) fn match_type(ty: String, args: &[usize]) -> SqlType {
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

    pub(crate) fn pg_parse_constraint(
        input: &'a str,
    ) -> IResult<&'a str, Constraint<'a>> {
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
        ))
        .parse(input)
    }

    pub(crate) fn parse_default(input: &str) -> IResult<&str, Constraint<'_>> {
        preceded(
            (tag_no_case("DEFAULT"), Self::parse_comment1),
            recognize(alt((
                delimited(
                    tag("\""),
                    recognize(many0(alt((
                        tag("\"\""),
                        tag("\\\""),
                        is_not("\\\""),
                    )))),
                    tag("\""),
                ),
                delimited(
                    tag("'"),
                    recognize(many0(alt((
                        tag("''"),
                        tag("\\'"),
                        is_not("\\'"),
                    )))),
                    tag("'"),
                ),
                Self::parse_parens,
                digit1,
            )))
            .map(Constraint::Def),
        )
        .parse(input)
    }

    pub(crate) fn pg_parse_inline_fk(
        input: &str,
    ) -> IResult<&str, Constraint<'_>> {
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

    pub(crate) fn pg_parse_fkaction(
        input: &str,
    ) -> IResult<&str, Constraint<'a>> {
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

    pub(crate) fn pg_parse_index(
        &mut self,
        is_unique: bool,
    ) -> Result<Created<'_>> {
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
                    |s| Ok((s, None)),
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

        let idx_method = self.parser(Self::pg_parse_index_method).map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "index method name".to_string(),
            },
            self.start_offset(),
        )?;

        let (_, mut index_method) = Self::match_idx_method(idx_method)
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidIndexMethod(
                        idx_method.unwrap_or_default().to_string(),
                    ),
                    idx_method
                        .map(|s| self.orig.offset(s))
                        .unwrap_or_else(|| self.start_offset()),
                )
            })?;

        self.parser(Self::parse_comment1).map_into(
            ErrorKind::NonWhitespace(self.next_token().to_string()),
            self.start_offset(),
        )?;

        let cols = self
            .parser(Self::parse_list(map_res(
                Self::parse_index_col,
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

        let pairs = self.parser(Self::pg_parse_with_params).map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "WITH clause".to_string(),
            },
            self.start_offset(),
        )?;

        Self::pg_apply_with_params(
            &mut index_method,
            pairs,
            self.start_offset(),
        )?;

        let predicate = self.pg_parse_where().map_into(
            ErrorKind::UnexpectedToken {
                found: self.next_token().to_string(),
                expected: "WHERE clause".to_string(),
            },
            self.start_offset(),
        )?;

        Ok(Created::Index { table_name, columns: cols, included, predicate })
    }

    pub(crate) fn pg_parse_index_method(
        input: &str,
    ) -> IResult<&str, Option<&str>> {
        opt(preceded(
            (tag_no_case("USING"), Self::parse_comment1),
            alphanumeric1,
        ))
        .parse(input)
    }

    pub(crate) fn match_idx_method(
        idx_method: Option<&str>,
    ) -> IResult<&str, Option<IndexMethod>> {
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
            return Err(nom::Err::Failure(nom::error::Error::new(
                "",
                nom::error::ErrorKind::Fail,
            )));
        }

        Ok(("", index_method))
    }

    pub(crate) fn parse_index_col(
        input: &'a str,
    ) -> IResult<&'a str, IdxCol<'a>> {
        (
            alt((
                recognize((
                    many1(alt((alphanumeric1, tag("_")))),
                    delimited(
                        (Self::parse_comment0, tag("("), Self::parse_comment0),
                        Self::parse_ident,
                        (Self::parse_comment0, tag(")")),
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
        )
            .parse(input)
    }

    pub(crate) fn pg_parse_include(&mut self) -> Result<Option<Vec<&'a str>>> {
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

    pub(crate) fn pg_parse_with_params(
        input: &'a str,
    ) -> IResult<&'a str, Option<Vec<(&'a str, &'a str)>>> {
        opt(preceded(
            (Self::parse_comment1, tag_no_case("WITH"), Self::parse_comment0),
            Self::parse_list(separated_pair(
                Self::parse_ident,
                (Self::parse_comment0, tag("="), Self::parse_comment0),
                alphanumeric1,
            )),
        ))
        .parse(input)
    }

    pub(crate) fn pg_apply_with_params(
        index_method: &mut Option<IndexMethod>,
        pairs: Option<Vec<(&str, &str)>>,
        position: usize,
    ) -> Result<()> {
        let mut o = IndexMethod::Other;
        let index_method: &mut IndexMethod = if pairs.is_some() {
            index_method.as_mut().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidIndexMethod("".to_string()),
                    position,
                )
            })?
        } else {
            index_method.as_mut().unwrap_or(&mut o)
        };

        for (key, value) in pairs.unwrap_or(vec![]) {
            let res = match key.to_lowercase().as_str() {
                "fillfactor" => {
                    let v = value.parse::<u8>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid fillfactor value".into(),
                            ),
                            position,
                        )
                    })?;
                    index_method.set_fillfactor(v)
                }

                "fastupdate" => {
                    let v = value.parse::<bool>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid fastupdate value".into(),
                            ),
                            position,
                        )
                    })?;
                    index_method.set_fastupdate(v)
                }

                "gin_pending_list_limit" => {
                    let v = value.parse::<u32>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid gin_pending_list_limit value".into(),
                            ),
                            position,
                        )
                    })?;
                    index_method.set_gin_pending_list_limit(v)
                }

                "buffering" => {
                    let v = match value.to_uppercase().as_str() {
                        "ON" | "TRUE" => GistBufMode::On,
                        "OFF" | "FALSE" => GistBufMode::Off,
                        "AUTO" => GistBufMode::Auto,
                        _ => {
                            return Err(Error::new(
                                ErrorKind::InvalidValue(value.to_string()),
                                position,
                            ));
                        }
                    };

                    index_method.set_buffering(v)
                }

                "autosummarize" => {
                    let v = match value.to_uppercase().as_str() {
                        "ON" | "TRUE" => true,
                        "OFF" | "FALSE" => false,
                        _ => {
                            return Err(Error::new(
                                ErrorKind::InvalidValue(value.to_string()),
                                position,
                            ));
                        }
                    };

                    index_method.set_autosummarize(v)
                }

                "pages_per_range" => {
                    let v = value.parse::<u32>().map_err(|_| {
                        Error::new(
                            ErrorKind::ParseFailure(
                                "invalid pages_per_range value".into(),
                            ),
                            position,
                        )
                    })?;
                    index_method.set_pages_per_range(v)
                }

                _ => Err(()),
            };

            res.map_err(|_| {
                Error::new(
                    ErrorKind::InvalidStorageParam {
                        key: key.to_string(),
                        value: value.to_string(),
                    },
                    position,
                )
            })?;
        }

        Ok(())
    }

    pub(crate) fn pg_parse_where(&mut self) -> Result<Option<String>> {
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

    pub(crate) fn parse_if_not_exists(&mut self) -> Result<bool> {
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

    pub(crate) fn parse_list<
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

    pub(crate) fn parse_parens(input: &str) -> IResult<&str, &str> {
        let mut depth = 0;
        let mut chars = input.char_indices().peekable();
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut escaped = false;

        while let Some((i, c)) = chars.next() {
            match c {
                '\\' => escaped = !escaped,

                '\'' if !in_double_quote
                    && chars.peek().is_some_and(|(_, ch)| *ch != '\'')
                    && !escaped =>
                {
                    in_single_quote = !in_single_quote;
                }

                '"' if !in_single_quote
                    && chars.peek().is_some_and(|(_, ch)| *ch != '"')
                    && !escaped =>
                {
                    in_double_quote = !in_double_quote;
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

    pub(crate) fn parse_ident(input: &str) -> IResult<&str, &str> {
        alt((
            delimited(
                tag("\""),
                recognize(many0(alt((
                    tag("\"\""),
                    tag("\\\""),
                    is_not("\\\""),
                )))),
                tag("\""),
            ),
            recognize((
                alt((tag("_"), alpha1)),
                many0(alt((tag("_"), alphanumeric1))),
            )),
        ))
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
        check: Option<String>,
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
pub(crate) enum OnEvent {
    Delete,
    Update,
}

#[derive(Clone, Debug)]
pub(crate) enum Constraint<'a> {
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
    use crate::{
        FkAction, IntervalField, SqlType, SupportedDBs, lexer::Constraint,
        lexer::Created, lexer::Lexer, lexer::OnEvent,
    };
    use nom::Parser;

    #[test]
    fn parse_ident_simple() {
        let (rem, id) = Lexer::parse_ident("users(id INT)").unwrap();
        assert_eq!(id, "users");
        assert!(rem.starts_with("("));
    }

    #[test]
    fn parse_ident_with_underscores() {
        let (rem, id) = Lexer::parse_ident("_my_table_name rest").unwrap();
        assert_eq!(id, "_my_table_name");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_ident_quoted() {
        let (rem, id) = Lexer::parse_ident("\"MyTable\" rest").unwrap();
        assert_eq!(id, "MyTable");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_ident_quoted_with_spaces() {
        let (rem, id) = Lexer::parse_ident("\"my table\" rest").unwrap();
        assert_eq!(id, "my table");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_ident_fails_on_number_start() {
        assert!(Lexer::parse_ident("123abc").is_err());
    }

    #[test]
    fn parse_comment0_empty() {
        let (rem, _) = Lexer::parse_comment0("").unwrap();
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_comment0_spaces() {
        let (rem, _) = Lexer::parse_comment0("   hello").unwrap();
        assert_eq!(rem, "hello");
    }

    #[test]
    fn parse_comment0_single_line_comment() {
        let (rem, _) = Lexer::parse_comment0("-- comment\nhello").unwrap();
        assert_eq!(rem, "hello");
    }

    #[test]
    fn parse_comment0_multi_line_comment() {
        let (rem, _) = Lexer::parse_comment0("/* comment */hello").unwrap();
        assert_eq!(rem, "hello");
    }

    #[test]
    fn parse_comment0_multiple_comments() {
        let (rem, _) = Lexer::parse_comment0("-- a\n/* b */  hello").unwrap();
        assert_eq!(rem, "hello");
    }

    #[test]
    fn parse_comment1_single_space() {
        let (rem, _) = Lexer::parse_comment1(" hello").unwrap();
        assert_eq!(rem, "hello");
    }

    #[test]
    fn parse_comment1_newline() {
        let (rem, _) = Lexer::parse_comment1("\nhello").unwrap();
        assert_eq!(rem, "hello");
    }

    #[test]
    fn parse_comment1_space_and_comment() {
        let (rem, _) = Lexer::parse_comment1(" /* hi */ hello").unwrap();
        assert_eq!(rem, "hello");
    }

    #[test]
    fn parse_comment1_fails_without_whitespace() {
        assert!(Lexer::parse_comment1("hello").is_err());
    }

    #[test]
    fn parse_parens_simple() {
        let (rem, inner) = Lexer::parse_parens("(a + b) rest").unwrap();
        assert_eq!(inner, "(a + b)");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_parens_nested() {
        let (rem, inner) = Lexer::parse_parens("(a + (b * c)) rest").unwrap();
        assert_eq!(inner, "(a + (b * c))");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_parens_with_single_quotes() {
        let (rem, inner) = Lexer::parse_parens("(')') rest").unwrap();
        assert_eq!(inner, "(')')");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_parens_with_double_quotes() {
        let (rem, inner) = Lexer::parse_parens("(\")\") rest").unwrap();
        assert_eq!(inner, "(\")\")");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_parens_with_escaped_single_quote() {
        let (rem, inner) = Lexer::parse_parens(r#"('\'') rest"#).unwrap();
        assert_eq!(inner, r#"('\'')"#);
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_parens_fails_unclosed() {
        assert!(Lexer::parse_parens("(unclosed").is_err());
    }

    #[test]
    fn parse_default_string_single_quotes() {
        let (rem, c) = Lexer::parse_default("DEFAULT 'hello' rest").unwrap();
        match c {
            Constraint::Def(s) => assert_eq!(s, "'hello'"),
            _ => panic!("expected Def constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_default_string_double_quotes() {
        let (rem, c) = Lexer::parse_default(r#"DEFAULT "hello" rest"#).unwrap();
        match c {
            Constraint::Def(s) => assert_eq!(s, r#""hello""#),
            _ => panic!("expected Def constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_default_function_call() {
        let (rem, c) =
            Lexer::parse_default("DEFAULT gen_random_uuid() rest").unwrap();
        match c {
            Constraint::Def(s) => assert_eq!(s, "gen_random_uuid()"),
            _ => panic!("expected Def constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_default_numeric() {
        let (rem, c) = Lexer::parse_default("DEFAULT 42 rest").unwrap();
        match c {
            Constraint::Def(s) => assert_eq!(s, "42"),
            _ => panic!("expected Def constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_inline_fk_table_only() {
        let (rem, c) =
            Lexer::pg_parse_inline_fk("REFERENCES users rest").unwrap();
        match c {
            Constraint::ForeignKey { table, col } => {
                assert_eq!(table, "users");
                assert!(col.is_none());
            }
            _ => panic!("expected ForeignKey constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_inline_fk_table_and_column() {
        let (rem, c) =
            Lexer::pg_parse_inline_fk("REFERENCES users(id) rest").unwrap();
        match c {
            Constraint::ForeignKey { table, col } => {
                assert_eq!(table, "users");
                assert_eq!(col, Some("id"));
            }
            _ => panic!("expected ForeignKey constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_fkaction_on_delete_cascade() {
        let (rem, c) =
            Lexer::pg_parse_fkaction("ON DELETE CASCADE rest").unwrap();
        match c {
            Constraint::FkAction { event, action } => {
                assert!(matches!(event, OnEvent::Delete));
                assert_eq!(action, FkAction::Cascade);
            }
            _ => panic!("expected FkAction constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_fkaction_on_update_restrict() {
        let (rem, c) =
            Lexer::pg_parse_fkaction("ON UPDATE RESTRICT rest").unwrap();
        match c {
            Constraint::FkAction { event, action } => {
                assert!(matches!(event, OnEvent::Update));
                assert_eq!(action, FkAction::Restrict);
            }
            _ => panic!("expected FkAction constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_fkaction_on_delete_set_null() {
        let (rem, c) =
            Lexer::pg_parse_fkaction("ON DELETE SET NULL rest").unwrap();
        match c {
            Constraint::FkAction { action, .. } => {
                assert_eq!(action, FkAction::SetNull);
            }
            _ => panic!("expected FkAction constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_fkaction_on_delete_set_default() {
        let (rem, c) =
            Lexer::pg_parse_fkaction("ON DELETE SET DEFAULT rest").unwrap();
        match c {
            Constraint::FkAction { action, .. } => {
                assert_eq!(action, FkAction::SetDefault);
            }
            _ => panic!("expected FkAction constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_fkaction_on_update_no_action() {
        let (rem, c) =
            Lexer::pg_parse_fkaction("ON UPDATE NO ACTION rest").unwrap();
        match c {
            Constraint::FkAction { action, .. } => {
                assert_eq!(action, FkAction::NoAction);
            }
            _ => panic!("expected FkAction constraint"),
        }
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_constraint_primary_key() {
        let (rem, c) = Lexer::pg_parse_constraint("PRIMARY KEY").unwrap();
        assert!(matches!(c, Constraint::PrimaryKey));
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_constraint_unique() {
        let (rem, c) = Lexer::pg_parse_constraint("UNIQUE").unwrap();
        assert!(matches!(c, Constraint::Unique));
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_constraint_not_null() {
        let (rem, c) = Lexer::pg_parse_constraint("NOT NULL").unwrap();
        assert!(matches!(c, Constraint::NotNull));
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_constraint_check() {
        let (rem, c) = Lexer::pg_parse_constraint("CHECK (x > 0)").unwrap();
        match c {
            Constraint::Check(s) => assert_eq!(s, "(x > 0)"),
            _ => panic!("expected Check constraint"),
        }
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_type_simple() {
        let (rem, (ty, args)) = Lexer::pg_parse_type("INT rest").unwrap();
        assert_eq!(ty, "INT");
        assert!(args.is_none());
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_double_precision() {
        let (rem, (ty, _)) =
            Lexer::pg_parse_type("DOUBLE PRECISION rest").unwrap();
        assert_eq!(ty, "DOUBLE PRECISION");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_timestamp_with_time_zone() {
        let (rem, (ty, _)) =
            Lexer::pg_parse_type("TIMESTAMP WITH TIME ZONE rest").unwrap();
        assert_eq!(ty, "TIMESTAMP WITH TIME ZONE");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_timestamp_without_time_zone() {
        let (rem, (ty, _)) =
            Lexer::pg_parse_type("TIMESTAMP WITHOUT TIME ZONE rest").unwrap();
        assert_eq!(ty, "TIMESTAMP WITHOUT TIME ZONE");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_character_varying() {
        let (rem, (ty, _)) =
            Lexer::pg_parse_type("CHARACTER VARYING rest").unwrap();
        assert_eq!(ty, "CHARACTER VARYING");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_with_args() {
        let (rem, (ty, args)) =
            Lexer::pg_parse_type("VARCHAR(255) rest").unwrap();
        assert_eq!(ty, "VARCHAR");
        assert_eq!(args, Some(vec![255]));
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_with_two_args() {
        let (rem, (ty, args)) =
            Lexer::pg_parse_type("DECIMAL(10, 2) rest").unwrap();
        assert_eq!(ty, "DECIMAL");
        assert_eq!(args, Some(vec![10, 2]));
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_interval_year() {
        let (rem, (ty, _)) =
            Lexer::pg_parse_type("INTERVAL YEAR rest").unwrap();
        assert_eq!(ty, "INTERVAL YEAR");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn parse_type_interval_day_to_second() {
        let (rem, (ty, _)) =
            Lexer::pg_parse_type("INTERVAL DAY TO SECOND rest").unwrap();
        assert_eq!(ty, "INTERVAL DAY TO SECOND");
        assert_eq!(rem, " rest");
    }

    #[test]
    fn match_type_integers() {
        assert!(matches!(
            Lexer::match_type("SMALLINT".into(), &[]),
            SqlType::SmallInt
        ));
        assert!(matches!(
            Lexer::match_type("INT2".into(), &[]),
            SqlType::SmallInt
        ));
        assert!(matches!(
            Lexer::match_type("INTEGER".into(), &[]),
            SqlType::Integer
        ));
        assert!(matches!(
            Lexer::match_type("INT".into(), &[]),
            SqlType::Integer
        ));
        assert!(matches!(
            Lexer::match_type("INT4".into(), &[]),
            SqlType::Integer
        ));
        assert!(matches!(
            Lexer::match_type("BIGINT".into(), &[]),
            SqlType::BigInt
        ));
        assert!(matches!(
            Lexer::match_type("INT8".into(), &[]),
            SqlType::BigInt
        ));
    }

    #[test]
    fn match_type_floats() {
        assert!(matches!(Lexer::match_type("REAL".into(), &[]), SqlType::Real));
        assert!(matches!(
            Lexer::match_type("FLOAT4".into(), &[]),
            SqlType::Real
        ));
        assert!(matches!(
            Lexer::match_type("DOUBLE PRECISION".into(), &[]),
            SqlType::DoublePrecision
        ));
        assert!(matches!(
            Lexer::match_type("FLOAT8".into(), &[]),
            SqlType::DoublePrecision
        ));
    }

    #[test]
    fn match_type_decimal_numeric() {
        match Lexer::match_type("DECIMAL".into(), &[10, 2]) {
            SqlType::Decimal(Some(10), Some(2)) => {}
            other => panic!("expected Decimal(10,2), got {other:?}"),
        }
        match Lexer::match_type("NUMERIC".into(), &[]) {
            SqlType::Numeric(None, None) => {}
            other => panic!("expected Numeric(None,None), got {other:?}"),
        }
    }

    #[test]
    fn match_type_strings() {
        assert!(matches!(
            Lexer::match_type("CHAR".into(), &[]),
            SqlType::Char(1)
        ));
        assert!(matches!(
            Lexer::match_type("CHAR".into(), &[50]),
            SqlType::Char(50)
        ));
        assert!(matches!(
            Lexer::match_type("CHARACTER".into(), &[]),
            SqlType::Char(1)
        ));
        assert!(matches!(
            Lexer::match_type("VARCHAR".into(), &[]),
            SqlType::VarChar(None)
        ));
        assert!(matches!(
            Lexer::match_type("VARCHAR".into(), &[255]),
            SqlType::VarChar(Some(255))
        ));
        assert!(matches!(
            Lexer::match_type("CHARACTER VARYING".into(), &[]),
            SqlType::VarChar(None)
        ));
        assert!(matches!(Lexer::match_type("TEXT".into(), &[]), SqlType::Text));
    }

    #[test]
    fn match_type_special() {
        assert!(matches!(
            Lexer::match_type("BYTEA".into(), &[]),
            SqlType::ByteA
        ));
        assert!(matches!(
            Lexer::match_type("BOOLEAN".into(), &[]),
            SqlType::Boolean
        ));
        assert!(matches!(
            Lexer::match_type("BOOL".into(), &[]),
            SqlType::Boolean
        ));
        assert!(matches!(Lexer::match_type("INET".into(), &[]), SqlType::Inet));
        assert!(matches!(Lexer::match_type("CIDR".into(), &[]), SqlType::Cidr));
        assert!(matches!(
            Lexer::match_type("MACADDR".into(), &[]),
            SqlType::MacAddr
        ));
        assert!(matches!(Lexer::match_type("JSON".into(), &[]), SqlType::Json));
        assert!(matches!(
            Lexer::match_type("JSONB".into(), &[]),
            SqlType::Jsonb
        ));
        assert!(matches!(Lexer::match_type("UUID".into(), &[]), SqlType::Uuid));
    }

    #[test]
    fn match_type_serials() {
        assert!(matches!(
            Lexer::match_type("SMALLSERIAL".into(), &[]),
            SqlType::SmallSerial
        ));
        assert!(matches!(
            Lexer::match_type("SERIAL2".into(), &[]),
            SqlType::SmallSerial
        ));
        assert!(matches!(
            Lexer::match_type("SERIAL".into(), &[]),
            SqlType::Serial
        ));
        assert!(matches!(
            Lexer::match_type("SERIAL4".into(), &[]),
            SqlType::Serial
        ));
        assert!(matches!(
            Lexer::match_type("BIGSERIAL".into(), &[]),
            SqlType::BigSerial
        ));
        assert!(matches!(
            Lexer::match_type("SERIAL8".into(), &[]),
            SqlType::BigSerial
        ));
    }

    #[test]
    fn match_type_timestamps() {
        assert!(matches!(
            Lexer::match_type("TIMESTAMP".into(), &[]),
            SqlType::Timestamp(None)
        ));
        assert!(matches!(
            Lexer::match_type("TIMESTAMP WITHOUT TIME ZONE".into(), &[]),
            SqlType::Timestamp(None)
        ));
        assert!(matches!(
            Lexer::match_type("TIMESTAMPTZ".into(), &[]),
            SqlType::Timestamptz(None)
        ));
        assert!(matches!(
            Lexer::match_type("TIMESTAMP WITH TIME ZONE".into(), &[]),
            SqlType::Timestamptz(None)
        ));
        assert!(matches!(Lexer::match_type("DATE".into(), &[]), SqlType::Date));
        assert!(matches!(
            Lexer::match_type("TIME".into(), &[]),
            SqlType::Time(None)
        ));
    }

    #[test]
    fn match_type_intervals() {
        match Lexer::match_type("INTERVAL YEAR".into(), &[]) {
            SqlType::Interval { fields, .. } => {
                assert!(matches!(fields, IntervalField::Year))
            }
            other => panic!("expected Interval, got {other:?}"),
        }
        match Lexer::match_type("INTERVAL DAY TO SECOND".into(), &[]) {
            SqlType::Interval { fields, .. } => {
                assert!(matches!(fields, IntervalField::DayToSecond))
            }
            other => panic!("expected Interval, got {other:?}"),
        }
        match Lexer::match_type("INTERVAL".into(), &[]) {
            SqlType::Interval { fields, .. } => {
                assert!(matches!(fields, IntervalField::None))
            }
            other => panic!("expected Interval, got {other:?}"),
        }
    }

    #[test]
    fn match_type_unknown() {
        match Lexer::match_type("CUSTOM_TYPE".into(), &[]) {
            SqlType::Unknown(s) => assert_eq!(s, "CUSTOM_TYPE"),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn parse_list_single() {
        let (rem, items) =
            Lexer::parse_list(Lexer::parse_ident).parse("(col1)").unwrap();
        assert_eq!(items, vec!["col1"]);
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_list_multiple() {
        let (rem, items) =
            Lexer::parse_list(Lexer::parse_ident).parse("(a, b, c)").unwrap();
        assert_eq!(items, vec!["a", "b", "c"]);
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_list_with_spaces() {
        let (rem, items) = Lexer::parse_list(Lexer::parse_ident)
            .parse("( col1 , col2 )")
            .unwrap();
        assert_eq!(items, vec!["col1", "col2"]);
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_list_with_comments() {
        let (rem, items) = Lexer::parse_list(Lexer::parse_ident)
            .parse("(col1 /* a */, col2 -- b\n )")
            .unwrap();
        assert_eq!(items, vec!["col1", "col2"]);
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_list_fails_empty() {
        assert!(Lexer::parse_list(Lexer::parse_ident).parse("()").is_err());
    }

    #[test]
    fn parse_if_not_exists_present() {
        let mut lexer = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: "IF NOT EXISTS users (id INT)",
            fks: vec![],
            orig: "IF NOT EXISTS users (id INT)",
        };
        let result = lexer.parse_if_not_exists().unwrap();
        assert!(result);
        assert!(lexer.statements.starts_with("users"));
    }

    #[test]
    fn parse_if_not_exists_absent() {
        let mut lexer = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: "users (id INT)",
            fks: vec![],
            orig: "users (id INT)",
        };
        let result = lexer.parse_if_not_exists().unwrap();
        assert!(!result);
        assert!(lexer.statements.starts_with("users"));
    }

    #[test]
    fn parse_if_not_exists_with_extra_spaces() {
        let mut lexer = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: "IF   NOT   EXISTS  users (id INT)",
            fks: vec![],
            orig: "IF   NOT   EXISTS  users (id INT)",
        };
        let result = lexer.parse_if_not_exists().unwrap();
        assert!(result);
        assert!(lexer.statements.starts_with("users"));
    }

    #[test]
    fn table_level_fk_single_col() {
        let input =
            "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE";
        let mut fks: Vec<(&str, Option<&str>)> = vec![];
        let (rem, iter) =
            Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 1);
        let (fk, col) = &results[0];
        assert_eq!(*col, "user_id");
        assert_eq!(fk.table, "users");
        assert_eq!(fk.column, Some("id".to_string()));
        assert_eq!(fk.on_delete, Some(FkAction::Cascade));
        assert_eq!(fk.on_update, None);
        assert_eq!(fks, vec![("users", Some("id"))]);
        assert_eq!(rem, "");
    }

    #[test]
    fn table_level_fk_multi_col() {
        let input = "FOREIGN KEY (a, b) REFERENCES other(x, y)";
        let mut fks: Vec<(&str, Option<&str>)> = vec![];
        let (rem, iter) =
            Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, "a");
        assert_eq!(results[0].0.table, "other");
        assert_eq!(results[0].0.column, Some("x".to_string()));
        assert_eq!(results[1].1, "b");
        assert_eq!(results[1].0.column, Some("y".to_string()));
        assert_eq!(fks.len(), 2);
        assert_eq!(rem, "");
    }

    #[test]
    fn table_level_fk_no_ref_col() {
        let input = "FOREIGN KEY (user_id) REFERENCES users";
        let mut fks: Vec<(&str, Option<&str>)> = vec![];
        let (rem, iter) =
            Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.column, None);
        assert_eq!(fks, vec![("users", None)]);
        assert_eq!(rem, "");
    }

    #[test]
    fn table_level_fk_with_on_update() {
        let input = "FOREIGN KEY (col) REFERENCES t(c) ON UPDATE RESTRICT";
        let mut fks: Vec<(&str, Option<&str>)> = vec![];
        let (rem, iter) =
            Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results[0].0.on_update, Some(FkAction::Restrict));
        assert_eq!(results[0].0.on_delete, None);
        assert_eq!(rem, "");
    }

    #[test]
    fn table_level_fk_with_both_actions() {
        let input = "FOREIGN KEY (col) REFERENCES t(c) ON DELETE SET NULL ON UPDATE CASCADE";
        let mut fks: Vec<(&str, Option<&str>)> = vec![];
        let (rem, iter) =
            Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
        let results: Vec<_> = iter.collect();
        let fk = &results[0].0;
        assert_eq!(fk.on_delete, Some(FkAction::SetNull));
        assert_eq!(fk.on_update, Some(FkAction::Cascade));
        assert_eq!(rem, "");
    }

    #[test]
    fn table_level_fk_with_comments() {
        let input =
            "FOREIGN KEY /* fk */ (user_id) -- note\n REFERENCES users(id)";
        let mut fks: Vec<(&str, Option<&str>)> = vec![];
        let (rem, iter) =
            Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "user_id");
        assert_eq!(rem, "");
    }

    #[test]
    fn table_level_unique_constraint() {
        let input = "CREATE TABLE test (id INT, UNIQUE (id))";
        let mut lexer = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: input,
            fks: vec![],
            orig: input,
        };
        let result = lexer.parse_statement().unwrap();
        assert!(matches!(result, Created::Table { .. }));
    }

    #[test]
    fn table_level_check_constraint() {
        let input = "CREATE TABLE test (id INT, CHECK (id > 0))";
        let mut lexer = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: input,
            fks: vec![],
            orig: input,
        };
        let result = lexer.parse_statement().unwrap();
        assert!(matches!(result, Created::Table { .. }));
    }

    #[test]
    fn parse_index_col_simple() {
        let (rem, (name, opclass, sort, null_order)) =
            Lexer::parse_index_col("col1").unwrap();
        assert_eq!(name, "col1");
        assert!(opclass.is_none());
        assert!(sort.is_none());
        assert!(null_order.is_none());
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_index_col_with_opclass_and_sort() {
        let (rem, (name, opclass, sort, null_order)) =
            Lexer::parse_index_col("col1 opclass DESC NULLS FIRST").unwrap();
        assert_eq!(name, "col1");
        assert_eq!(opclass.unwrap(), "opclass");
        assert_eq!(sort.unwrap(), crate::IndexSortOrder::Desc);
        assert_eq!(null_order.unwrap(), crate::IndexNullOrder::NullsFirst);
        assert!(rem.is_empty());
    }

    #[test]
    fn parse_index_col_with_expression() {
        let (rem, (name, opclass, sort, null_order)) =
            Lexer::parse_index_col("col1(expr) opclass").unwrap();
        assert_eq!(name, "col1(expr)");
        assert_eq!(opclass.unwrap(), "opclass");
        assert!(sort.is_none());
        assert!(null_order.is_none());
        assert!(rem.is_empty());
    }
}
