use crate::{IntervalField, LexError, SqlColumn, SqlType};

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alphanumeric1, digit1, multispace0, multispace1},
    combinator::{map_res, opt, recognize, value},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, preceded},
};

pub(crate) fn parse_statement(db: SupportedDBs, input: &str) -> IResult<&str, Created> {
    let (input, _) = tag_no_case("CREATE")(input)?;

    let (input, _) = multispace1(input)?;

    let (input, output) = alt((tag_no_case("TABLE"), tag_no_case("INDEX"))).parse(input)?;

    match output.to_uppercase().as_str() {
        "TABLE" => {
            let (input, _) = multispace1(input)?;

            let (input, _) = opt((
                multispace1,
                tag_no_case("IF"),
                multispace1,
                tag_no_case("NOT"),
                multispace1,
                tag_no_case("EXISTS"),
            ))
            .parse(input)?;

            let (input, _) = multispace1(input)?;
            let (input, table_name) = parse_ident(input)?;
            let (input, _) = multispace0(input)?;

            let (input, cols) =
                delimited(tag("("), separated_list1((multispace0, tag(","), multispace0), parse_col_def), tag(")"))
                    .parse(input)?;

            Ok((
                input,
                Created::Table {
                    name: table_name.to_string(),
                    columns: cols,
                },
            ))
        }

        // temporary return
        "INDEX" => Ok((
            input,
            Created::Table {
                name: "col".to_string(),
                columns: vec![SqlColumn {
                    name: "col".to_string(),
                    sql_type: SqlType::SmallInt,
                    is_indexed: false,
                    not_null: false,
                }],
            },
        )),

        _ => unreachable!(),
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
        "DECIMAL" => SqlType::Decimal(args.get(0).copied(), args.get(1).copied()),
        "NUMERIC" => SqlType::Numeric(args.get(0).copied(), args.get(1).copied()),

        // String/Text
        "CHAR" | "CHARACTER" => SqlType::Char(args.get(0).copied().unwrap_or(1)),
        "VARCHAR" | "CHARACTER VARYING" => SqlType::VarChar(args.get(0).copied()),
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

    pk.unwrap_or(Vec::new()).iter().for_each(|s| match *s {
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
        name: String,
        table_name: String,
        columns: Vec<String>,
    },
}

pub enum SupportedDBs {
    Postgres,
}
