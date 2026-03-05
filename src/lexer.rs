use crate::{
    Error, GistBufMode, IndexMethod, IndexNullOrder, IndexSortOrder, IntervalField, Result, SqlColumn, SqlIndexColumn, SqlType, SupportedDBs
};

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alphanumeric1, digit1, multispace0, multispace1},
    combinator::{map_res, opt, recognize, value},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, preceded, separated_pair, terminated},
};

pub(crate) fn parse_statement(
    _db: SupportedDBs,
    input: &str,
) -> Result<(&str, Created)> {
    let (input, _) = multispace0(input)?;

    let (input, _) = tag_no_case("CREATE")(input)?;

    let (input, _) = multispace1(input)?;

    let (input, output) = alt((
        tag_no_case("TABLE"),
        recognize((opt(tag_no_case("UNIQUE")), tag_no_case("INDEX"))),
    ))
    .parse(input)?;

    let output = output.to_uppercase();

    match output.as_str() {
        "TABLE" => {
            let (input, _) = multispace1(input)?;

            let (input, _) = opt((
                tag_no_case("IF"),
                multispace1,
                tag_no_case("NOT"),
                multispace1,
                tag_no_case("EXISTS"),
                multispace1,
            ))
            .parse(input)?;

            let (input, table_name) = parse_ident(input)?;
            let (input, _) = multispace0(input)?;

            let (input, cols) = delimited(
                tag("("),
                separated_list1(
                    (multispace0, tag(","), multispace0),
                    parse_col_def,
                ),
                tag(")"),
            )
            .parse(input)?;

            Ok((
                input,
                Created::Table { name: table_name.to_string(), columns: cols },
            ))
        }

        _ => {
            let is_unique = &output[..6] == "UNIQUE";

            let (input, _) = multispace1(input)?;

            let (input, concurrent) =
                opt(terminated(tag_no_case("CONCURRENTLY"), multispace1))
                    .parse(input)?;
            let concurrent = concurrent.is_some();

            let (input, _) = multispace1(input)?;

            let (input, _) = opt((
                tag_no_case("IF"),
                multispace1,
                tag_no_case("NOT"),
                multispace1,
                tag_no_case("EXISTS"),
                multispace1,
            ))
            .parse(input)?;

            let (input, index_name) =
                opt(terminated(parse_ident, multispace1)).parse(input)?;

            let (input, table_name) =
                preceded((tag_no_case("ON"), multispace1), parse_ident)
                    .parse(input)?;

            let (input, _) = multispace1(input)?;

            let (input, idx_method) = opt(preceded(
                (tag_no_case("USING"), multispace1),
                alphanumeric1,
            ))
            .parse(input)?;

            let mut index_method =
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
                return Err(Error::InvalidMethod(idx_method.unwrap_or_default().into()));
            }

            let (input, _) = multispace1(input)?;

            let (input, cols) = delimited(
                tag("("),
                separated_list1(
                    (multispace0, tag(","), multispace0),
                    map_res(
                        (
                            alt((
                                recognize((
                                    alphanumeric1,
                                    multispace0,
                                    tag("("),
                                    multispace0,
                                    parse_ident,
                                    multispace0,
                                    tag(")"),
                                )),
                                parse_ident,
                            )),

                            opt(preceded(multispace1, parse_ident)),
                            opt(preceded(
                                multispace1,
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
                                multispace1,
                                alt((
                                    value(
                                        IndexNullOrder::NullsFirst,
                                        (
                                            tag_no_case("NULLS"),
                                            multispace1,
                                            tag_no_case("FIRST"),
                                        ),
                                    ),
                                    value(
                                        IndexNullOrder::NullsLast,
                                        (
                                            tag_no_case("NULLS"),
                                            multispace1,
                                            tag_no_case("LAST"),
                                        ),
                                    ),
                                )),
                            )),
                        ),
                        |(name, opclass, sort1, sort2)| {
                            Ok::<
                                SqlIndexColumn,
                                nom::Err<nom::error::Error<&str>>,
                            >(SqlIndexColumn {
                                name: name.to_string(),
                                opclass: opclass.map(String::from),
                                sort_order: sort1,
                                null_order: sort2,
                            })
                        },
                    ),
                ),
                tag(")"),
            )
            .parse(input)?;

            let (input, _) = multispace1(input)?;

            let (input, pairs) = opt(preceded(
                (tag_no_case("WITH"), multispace1),
                delimited(
                    tag("("),
                    separated_list1(
                        (multispace0, tag(","), multispace0),
                        separated_pair(
                            parse_ident,
                            (multispace0, tag("="), multispace0),
                            alphanumeric1,
                        ),
                    ),
                    tag(")"),
                ),
            ))
            .parse(input)?;

            if pairs.is_some() {
                idx_method.ok_or(Error::UnexpectedToken(input.into(), "Index Method".into()))?;
            }

            for (key, value) in pairs.unwrap_or(vec![]) {
                let res = match key.to_lowercase().as_str() {
                    "fillfactor" => {
                        let v = value.parse()?;

                        let mut r = Ok(());
                        index_method.as_mut().map(|m| r = m.set_fillfactor(v));
                        r
                    }

                    "fastupdate" => {
                        let v = value.parse()?;

                        let mut r = Ok(());
                        index_method.as_mut().map(|m| r = m.set_fastupdate(v));
                        r
                    }

                    "gin_pending_list_limit" => {
                        let v = value.parse()?;

                        let mut r = Ok(());
                        index_method
                            .as_mut()
                            .map(|m| r = m.set_gin_pending_list_limit(v));
                        r
                    }

                    "buffering" => {
                        let v = match value.to_uppercase().as_str() {
                            "ON" | "TRUE" => GistBufMode::On,
                            "OFF" | "FALSE" => GistBufMode::Off,
                            "AUTO" => GistBufMode::Auto,
                            _ => {
                                return Err(Error::ParseFailure(value.into()));
                            }
                        };

                        let mut r = Ok(());
                        index_method.as_mut().map(|m| r = m.set_buffering(v));
                        r
                    }

                    "autosummarize" => {
                        let v = match value.to_uppercase().as_str() {
                            "ON" | "TRUE" => true,
                            "OFF" | "FALSE" => false,
                            _ => return Err(Error::ParseFailure(value.into())),
                        };

                        let mut r = Ok(());
                        index_method
                            .as_mut()
                            .map(|m| r = m.set_autosummarize(v));
                        r
                    }

                    "pages_per_range" => {
                        let v = value.parse()?;

                        let mut r = Ok(());
                        index_method
                            .as_mut()
                            .map(|m| r = m.set_pages_per_range(v));
                        r
                    }

                    _ => Err(()),
                };

                res.map_err(|_| {
                    Error::InvalidParam(key.into())
                })?;
            }

            // temporary return
            Ok((
                input,
                Created::Index {
                    name: index_name.map(String::from),
                    table_name: table_name.to_string(),
                    method: index_method,
                    columns: cols,
                    concurrent,
                    is_unique,
                },
            ))
        }
    }
}

fn parse_col_def(input: &str) -> IResult<&str, SqlColumn> {
    let (input, col_name) = parse_ident(input)?;
    let (input, _) = multispace1(input)?;

    let (input, sql_type) = alt((
        tag_no_case("DOUBLE PRECISION"),
        tag_no_case("TIMESTAMP WITH TIME ZONE"),
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

    let (input, args) = opt((
        multispace0,
        delimited(
            tag("("),
            separated_list1(
                (multispace0, tag(","), multispace0),
                map_res(digit1, |s: &str| s.parse::<usize>()),
            ),
            tag(")"),
        ),
    ))
    .parse(input)?;

    let (_, args) = args.unwrap_or(("", Vec::new()));

    let ty = sql_type.to_uppercase();
    let sql_type = match ty.as_str() {
        // Numeric
        "SMALLINT" | "INT2" => SqlType::SmallInt,
        "INTEGER" | "INT" | "INT4" => SqlType::Integer,
        "BIGINT" | "INT8" => SqlType::BigInt,
        "REAL" | "FLOAT4" => SqlType::Real,
        "DOUBLE PRECISION" | "FLOAT8" => SqlType::DoublePrecision,
        "DECIMAL" => {
            SqlType::Decimal(args.get(0).copied(), args.get(1).copied())
        }
        "NUMERIC" => {
            SqlType::Numeric(args.get(0).copied(), args.get(1).copied())
        }

        // String/Text
        "CHAR" | "CHARACTER" => {
            SqlType::Char(args.get(0).copied().unwrap_or(1))
        }
        "VARCHAR" | "CHARACTER VARYING" => {
            SqlType::VarChar(args.get(0).copied())
        }
        "TEXT" => SqlType::Text,

        // Binary
        "BYTEA" => SqlType::ByteA,

        // Date/Time
        "TIMESTAMP" => SqlType::Timestamp(args.get(0).copied()),
        "TIMESTAMPTZ" => SqlType::Timestamptz(args.get(0).copied()),
        "DATE" => SqlType::Date,
        "TIME" => SqlType::Time(args.get(0).copied()),
        _ if &ty[0..7] == "INTERVAL" => SqlType::Interval {
            fields: match ty.as_str() {
                "INTERVAL YEAR" => IntervalField::Year,
                "INTERVAL MONTH" => IntervalField::Month,
                "INTERVAL DAY" => IntervalField::Day,
                "INTERVAL HOUR" => IntervalField::Hour,
                "INTERVAL MINUTE" => IntervalField::Minute,
                "INTERVAL SECOND" => IntervalField::Second,
                "INTERVAL YEAR TO MONTH" => IntervalField::YearToMonth,
                "INTERVAL DAY TO HOUR" => IntervalField::DayToHour,
                "INTERVAL DAY TO MINUTE" => IntervalField::DayToMinute,
                "INTERVAL DAY TO SECOND" => IntervalField::DayToSecond,
                "INTERVAL HOUR TO MINUTE" => IntervalField::HourToMinute,
                "INTERVAL HOUR TO SECOND" => IntervalField::HourToSecond,
                "INTERVAL MINUTE TO SECOND" => IntervalField::MinuteToSecond,
                "INTERVAL" => IntervalField::None,
                _ => unreachable!(),
            },

            precision: args.get(0).copied(),
        },

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

        _ => {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::IsNot,
            )));
        }
    };

    let (input, pk) = opt(preceded(
        multispace1,
        many0(alt((
            tag_no_case("PRIMARY KEY"),
            tag_no_case("UNIQUE"),
            tag_no_case("NOT NULL"),
            recognize(many1(alt((multispace0, alphanumeric1)))),
        ))),
    ))
    .parse(input)?;

    let mut is_indexed = false;
    let mut not_null = false;

    pk.as_deref().unwrap_or(&[]).iter().for_each(|s| match *s {
        "PRIMARY KEY" => {
            is_indexed = true;
            not_null = true;
        }

        "UNIQUE" => is_indexed = true,

        "NOT NULL" => not_null = true,

        _ => {}
    });

    Ok((
        input,
        SqlColumn {
            name: col_name.to_string(),
            sql_type,
            is_indexed,
            not_null,
        },
    ))
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

pub(crate) enum Created {
    Table {
        name: String,
        columns: Vec<SqlColumn>,
    },

    Index {
        name: Option<String>,
        table_name: String,
        method: Option<IndexMethod>,
        columns: Vec<SqlIndexColumn>,
        concurrent: bool,
        is_unique: bool,
    },
}
