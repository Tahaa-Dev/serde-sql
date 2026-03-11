use indexmap::IndexMap;
use nom::{
    Parser, bytes::complete::tag, character::complete::multispace0,
};

use crate::{
    Error, Result, SqlColumn, SupportedDBs, lexer::{Created, parse_statement}
};

pub type Map = IndexMap<String, SqlColumn>;

pub struct SqlTable {
    columns: Map,
    primary_key: Option<String>,
}

impl SqlTable {
    fn from_sql(db: SupportedDBs, statement: &str) -> Result<(&str, (Self, String))> {
        let (input, created) = parse_statement(db, statement)?;
        let (input, _) = (multispace0, tag(";"), multispace0).parse(input)?;

        Ok((
            input,
            match created {
                Created::Table { name, columns, primary_key } => {
                    (Self { columns, primary_key }, name)
                }

                _ => return Err(Error::UnexpectedToken("INDEX".to_string(), "TABLE".to_string())),
            },
        ))
    }
}
