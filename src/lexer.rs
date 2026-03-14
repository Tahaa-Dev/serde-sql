#![allow(clippy::manual_inspect)] // Cannot use inspect since it returns immutable references

use crate::{
    ColMap, Error, GistBufMode, IndexMethod, IndexNullOrder, IndexSortOrder,
    IntervalField, Result, SqlColumn, SqlIndexColumn, SqlType, SupportedDBs,
};

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{is_not, tag, tag_no_case, take_until},
    character::complete::{
        alphanumeric1, digit1, multispace0, multispace1, none_of,
    },
    combinator::{map_res, not, opt, recognize, value},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, preceded, separated_pair, terminated},
};

pub(crate) struct Lexer<'a> {
    #[allow(dead_code)]
    pub(crate) db: SupportedDBs,
    pub(crate) statements: &'a str,
}

impl<'a> Lexer<'a> {
    pub(crate) fn parse_statement(&mut self) -> Result<Created<'_>> {
        self.parser(parse_comment0)?;

        self.parser(tag_no_case("CREATE"))?;

        self.parser(parse_comment1)?;

        let output = self.parser(alt((
            tag_no_case("TABLE"),
            recognize((
                opt((tag_no_case("UNIQUE"), parse_comment1)),
                tag_no_case("INDEX"),
            )),
        )))?;

        let output = output.to_uppercase();

        match output.as_str() {
            "TABLE" => {
                let mut columns = ColMap::new();

                let mut primary_key = None;

                let parse_col_def = |input| {
                    let (input, col_name) = parse_ident(input)?;
                    let (input, _) = parse_comment1(input)?;

                    let (input, (sql_type, args)) = Self::pg_parse_type(input)?;

                    let args = args.unwrap_or(Vec::new());

                    let ty = sql_type.to_uppercase();

                    let sql_type = match ty.as_str() {
                        // Numeric
                        "SMALLINT" | "INT2" => SqlType::SmallInt,
                        "INTEGER" | "INT" | "INT4" => SqlType::Integer,
                        "BIGINT" | "INT8" => SqlType::BigInt,
                        "REAL" | "FLOAT4" => SqlType::Real,
                        "DOUBLE PRECISION" | "FLOAT8" => {
                            SqlType::DoublePrecision
                        }
                        "DECIMAL" => SqlType::Decimal(
                            args.first().copied(),
                            args.get(1).copied(),
                        ),
                        "NUMERIC" => SqlType::Numeric(
                            args.first().copied(),
                            args.get(1).copied(),
                        ),

                        // String/Text
                        "CHAR" | "CHARACTER" => {
                            SqlType::Char(args.first().copied().unwrap_or(1))
                        }
                        "VARCHAR" | "CHARACTER VARYING" => {
                            SqlType::VarChar(args.first().copied())
                        }
                        "TEXT" => SqlType::Text,

                        // Binary
                        "BYTEA" => SqlType::ByteA,

                        // Boolean
                        "BOOLEAN" | "BOOL" => SqlType::Boolean,

                        // Network
                        "INET" => SqlType::Inet,
                        "CIDR" => SqlType::Cidr,
                        "MACADDR" => SqlType::MacAddr,

                        // Semi-Structured
                        "JSON" => SqlType::Json,
                        "JSONB" => SqlType::Jsonb,
                        "UUID" => SqlType::Uuid,

                        // Serial
                        "SMALLSERIAL" | "SERIAL2" => SqlType::SmallSerial,
                        "SERIAL" | "SERIAL4" => SqlType::Serial,
                        "BIGSERIAL" | "SERIAL8" => SqlType::BigSerial,

                        // Date/Time
                        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
                            SqlType::Timestamp(args.first().copied())
                        }
                        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
                            SqlType::Timestamptz(args.first().copied())
                        }
                        "DATE" => SqlType::Date,
                        "TIME" => SqlType::Time(args.first().copied()),
                        _ if &ty[0..7] == "INTERVAL" => SqlType::Interval {
                            fields: match ty.as_str() {
                                "INTERVAL YEAR" => IntervalField::Year,
                                "INTERVAL MONTH" => IntervalField::Month,
                                "INTERVAL DAY" => IntervalField::Day,
                                "INTERVAL HOUR" => IntervalField::Hour,
                                "INTERVAL MINUTE" => IntervalField::Minute,
                                "INTERVAL SECOND" => IntervalField::Second,
                                "INTERVAL YEAR TO MONTH" => {
                                    IntervalField::YearToMonth
                                }
                                "INTERVAL DAY TO HOUR" => {
                                    IntervalField::DayToHour
                                }
                                "INTERVAL DAY TO MINUTE" => {
                                    IntervalField::DayToMinute
                                }
                                "INTERVAL DAY TO SECOND" => {
                                    IntervalField::DayToSecond
                                }
                                "INTERVAL HOUR TO MINUTE" => {
                                    IntervalField::HourToMinute
                                }
                                "INTERVAL HOUR TO SECOND" => {
                                    IntervalField::HourToSecond
                                }
                                "INTERVAL MINUTE TO SECOND" => {
                                    IntervalField::MinuteToSecond
                                }
                                "INTERVAL" => IntervalField::None,
                                _ => unreachable!(),
                            },

                            precision: args.first().copied(),
                        },

                        _ => {
                            return Err(nom::Err::Failure(
                                nom::error::Error::new(
                                    input,
                                    nom::error::ErrorKind::IsNot,
                                ),
                            ));
                        }
                    };

                    let mut is_primary_key = false;
                    let mut not_null = false;
                    let mut index = None;

                    let (input, pk) = Self::pg_parse_constraints(input)?;

                    for s in pk.unwrap_or(vec![]) {
                        match s.to_uppercase().as_str() {
                            "PRIMARY KEY" => {
                                not_null = true;
                                is_primary_key = true;

                                if primary_key.is_some() {
                                    return Err(nom::Err::Failure(
                                        nom::error::Error::new(
                                            s,
                                            nom::error::ErrorKind::OneOf,
                                        ),
                                    ));
                                } else {
                                    primary_key = Some(col_name.to_string());
                                }

                                index = Some(SqlIndexColumn::default());
                            }

                            "UNIQUE" => index = Some(SqlIndexColumn::default()),

                            "NOT NULL" => not_null = true,

                            _ => {}
                        }
                    }

                    if !columns.contains_key(col_name) {
                        columns.insert(
                            col_name.to_string(),
                            SqlColumn {
                                sql_type,
                                index,
                                not_null,
                                is_primary_key,
                            },
                        );
                    } else {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            col_name,
                            nom::error::ErrorKind::OneOf,
                        )));
                    }

                    Ok((input, ()))
                };

                self.parser(parse_comment1)?;

                self.parser(opt((
                    tag_no_case("IF"),
                    parse_comment1,
                    tag_no_case("NOT"),
                    parse_comment1,
                    tag_no_case("EXISTS"),
                    parse_comment1,
                )))?;

                let table_name = self.parser(parse_ident)?;
                self.parser(parse_comment0)?;

                self.parser(delimited(
                    (tag("("), parse_comment0),
                    separated_list1(
                        (parse_comment0, tag(","), parse_comment0),
                        parse_col_def,
                    ),
                    (parse_comment0, tag(")")),
                ))?;

                Ok(Created::Table {
                    name: table_name.to_string(),
                    columns,
                    primary_key,
                })
            }

            _ => {
                let is_unique = output.starts_with("UNIQUE");

                self.parser(parse_comment1)?;

                let concurrent = self.parser(opt(terminated(
                    tag_no_case("CONCURRENTLY"),
                    parse_comment1,
                )))?;

                let is_concurrent = concurrent.is_some();

                self.parser(opt((
                    tag_no_case("IF"),
                    parse_comment1,
                    tag_no_case("NOT"),
                    parse_comment1,
                    tag_no_case("EXISTS"),
                    parse_comment1,
                )))?;

                let (index_name, table_name): (Option<&str>, &str) = self
                    .parser(alt((
                        (
                            opt(terminated(parse_ident, parse_comment1)),
                            preceded(
                                (tag_no_case("ON"), parse_comment1),
                                parse_ident,
                            ),
                        ),
                        (
                            opt(recognize(not(is_not("")))),
                            preceded(
                                (tag_no_case("ON"), parse_comment1),
                                parse_ident,
                            ),
                        ),
                    )))?;

                self.parser(parse_comment1)?;

                let idx_method = self.parser(opt(preceded(
                    (tag_no_case("USING"), parse_comment1),
                    alphanumeric1,
                )))?;

                let mut index_method =
                    idx_method.map(|s: &str| match s.to_uppercase().as_str() {
                        "BTREE" => IndexMethod::BTree { fillfactor: None },
                        "HASH" => IndexMethod::Hash { fillfactor: None },
                        "GIN" => IndexMethod::Gin {
                            fastupdate: None,
                            gin_pending_list_limit: None,
                        },
                        "GIST" => IndexMethod::Gist {
                            fillfactor: None,
                            buffering: None,
                        },
                        "BRIN" => IndexMethod::Brin {
                            pages_per_range: None,
                            autosummarize: None,
                        },
                        "SPGIST" => IndexMethod::SpGist { fillfactor: None },
                        _ => IndexMethod::Other,
                    });

                if let Some(IndexMethod::Other) = index_method {
                    return Err(Error::InvalidMethod(
                        idx_method.unwrap_or_default().into(),
                    ));
                }

                self.parser(parse_comment1)?;

                let cols = self.parser(delimited(
                    (tag("("), parse_comment0),
                    separated_list1(
                        (parse_comment0, tag(","), parse_comment0),
                        map_res(
                            (
                                alt((
                                    recognize((
                                        alphanumeric1,
                                        parse_comment0,
                                        tag("("),
                                        parse_comment0,
                                        parse_ident,
                                        parse_comment0,
                                        tag(")"),
                                    )),
                                    parse_ident,
                                )),
                                opt(preceded(parse_comment1, parse_ident)),
                                opt(preceded(
                                    parse_comment1,
                                    alt((
                                        value(
                                            IndexSortOrder::Asc,
                                            tag_no_case("ASC"),
                                        ),
                                        value(
                                            IndexSortOrder::Desc,
                                            tag_no_case("DESC"),
                                        ),
                                    )),
                                )),
                                opt(preceded(
                                    parse_comment1,
                                    alt((
                                        value(
                                            IndexNullOrder::NullsFirst,
                                            (
                                                tag_no_case("NULLS"),
                                                parse_comment1,
                                                tag_no_case("FIRST"),
                                            ),
                                        ),
                                        value(
                                            IndexNullOrder::NullsLast,
                                            (
                                                tag_no_case("NULLS"),
                                                parse_comment1,
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
                                    },
                                ))
                            },
                        ),
                    ),
                    (parse_comment0, tag(")")),
                ))?;

                let pairs = self.parser(opt(preceded(
                    (parse_comment1, tag_no_case("WITH"), parse_comment1),
                    delimited(
                        (tag("("), parse_comment0),
                        separated_list1(
                            (parse_comment0, tag(","), parse_comment0),
                            separated_pair(
                                parse_ident,
                                (parse_comment0, tag("="), parse_comment0),
                                alphanumeric1,
                            ),
                        ),
                        (parse_comment0, tag(")")),
                    ),
                )))?;

                if pairs.is_some() {
                    idx_method.ok_or(Error::UnexpectedToken(
                        self.statements.into(),
                        "Index Method".into(),
                    ))?;
                }

                for (key, value) in pairs.unwrap_or(vec![]) {
                    let res = match key.to_lowercase().as_str() {
                        "fillfactor" => {
                            let v = value.parse()?;

                            let mut r = Ok(());
                            index_method.as_mut().map(|m| {
                                r = m.set_fillfactor(v);
                                m
                            });
                            r
                        }

                        "fastupdate" => {
                            let v = value.parse()?;

                            let mut r = Ok(());
                            index_method.as_mut().map(|m| {
                                r = m.set_fastupdate(v);
                                m
                            });
                            r
                        }

                        "gin_pending_list_limit" => {
                            let v = value.parse()?;

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
                                    return Err(Error::ParseFailure(
                                        value.into(),
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
                                    return Err(Error::ParseFailure(
                                        value.into(),
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
                            let v = value.parse()?;

                            let mut r = Ok(());

                            index_method.as_mut().map(|m| {
                                r = m.set_pages_per_range(v);
                                m
                            });
                            r
                        }

                        _ => Err(()),
                    };

                    res.map_err(|_| Error::InvalidParam(key.into()))?;
                }

                Ok(Created::Index { table_name, columns: cols })
            }
        }
    }

    pub(crate) fn parser<
        T,
        F: Parser<&'a str, Output = T, Error = nom::error::Error<&'a str>>,
    >(
        &mut self,
        mut parser: F,
    ) -> Result<T> {
        let (input, out) = parser.parse(self.statements)?;

        self.statements = input;

        Ok(out)
    }

    fn pg_parse_constraints(input: &str) -> IResult<&str, Option<Vec<&str>>> {
        opt(many0(preceded(
            parse_comment1,
            alt((
                tag_no_case("PRIMARY KEY"),
                tag_no_case("UNIQUE"),
                tag_no_case("NOT NULL"),
                recognize(many0(alt((
                    alphanumeric1,
                    multispace1,
                    tag("_"),
                    tag("()"),
                    delimited(tag("("), is_not(")"), tag(")")),
                    delimited(tag("'"), is_not("'"), tag("'")),
                    delimited(tag("\""), is_not("\""), tag("\"")),
                )))),
            )),
        )))
        .parse(input)
    }

    fn pg_parse_type(input: &str) -> IResult<&str, (&str, Option<Vec<usize>>)> {
        let (input, sql_type) = alt((
            tag_no_case("DOUBLE PRECISION"),
            tag_no_case("TIMESTAMP WITH TIME ZONE"),
            tag_no_case("TIMESTAMP WITHOUT TIME ZONE"),
            tag_no_case("CHARACTER VARYING"),
            tag_no_case("INTERVAL YEAR"),
            tag_no_case("INTERVAL MONTH"),
            tag_no_case("INTERVAL DAY"),
            tag_no_case("INTERVAL HOUR"),
            tag_no_case("INTERVAL MINUTE"),
            tag_no_case("INTERVAL SECOND"),
            tag_no_case("INTERVAL YEAR TO MONTH"),
            tag_no_case("INTERVAL DAY TO HOUR"),
            tag_no_case("INTERVAL DAY TO MINUTE"),
            tag_no_case("INTERVAL DAY TO SECOND"),
            tag_no_case("INTERVAL HOUR TO MINUTE"),
            tag_no_case("INTERVAL HOUR TO SECOND"),
            tag_no_case("INTERVAL MINUTE TO SECOND"),
            alphanumeric1,
        ))
        .parse(input)?;

        let (input, args) = opt(preceded(
            parse_comment0,
            delimited(
                (tag("("), parse_comment0),
                separated_list1(
                    (parse_comment0, tag(","), parse_comment0),
                    map_res(digit1, |s: &str| s.parse::<usize>()),
                ),
                (parse_comment0, tag(")")),
            ),
        ))
        .parse(input)?;

        Ok((input, (sql_type, args)))
    }
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

pub(crate) fn parse_comment0(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(alt((
        // many0(none_of("\r\n")) instead of is_not("\r\n") is because is_not fails if the pattern
        // is not found while many0(none_of) doesn't which is needed since the comment could be at
        // EOF where is_not wouldn't find \n or \r and it would fail
        value((), (tag("--"), many0(none_of("\r\n")), multispace0)),
        value((), (tag("/*"), take_until("*/"), tag("*/"), multispace0)),
    )))
    .parse(input)?;

    Ok((input, ()))
}

pub(crate) fn parse_comment1(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace1(input)?;
    let (input, _) = opt(alt((
        value((), (tag("--"), many0(none_of("\r\n")), multispace1)),
        value((), (tag("/*"), take_until("*/"), tag("*/"), multispace1)),
    )))
    .parse(input)?;

    Ok((input, ()))
}

pub(crate) enum Created<'a> {
    Table { name: String, columns: ColMap, primary_key: Option<String> },

    Index { table_name: &'a str, columns: Vec<(&'a str, SqlIndexColumn)> },
}

#[cfg(test)]
mod tests {
    use crate::{SupportedDBs, lexer::Lexer};

    #[test]
    fn lexer_valid() {
        let mut lexer1 = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: r#"CREATE table IF NOT EXISTS -- Make sure it's only created once
            users (
                id UUID primary key, /* Primary key Notes */
                name TEXT
            )"#,
        };

        lexer1.parse_statement().unwrap();

        let mut lexer2 = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: r#"CREATE InDex -- Note
            IF NOT EXISTS user_id ON users USING BRIN (
                id, /* Multi-line
                    Note */
                name
            )"#,
        };

        lexer2.parse_statement().unwrap();

        assert!(lexer1.statements.is_empty());
        assert!(lexer2.statements.is_empty());
    }

    #[test]
    #[should_panic]
    fn lexer_invalid() {
        let mut lexer = Lexer {
            db: SupportedDBs::PostgreSQL,
            statements: r#"CREATE INDEX should_fail ON users WITH"#,
        };

        lexer.parse_statement().unwrap();
    }
}
