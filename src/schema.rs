use hashbrown::HashMap;
use indexmap::IndexMap;
use nom::{Parser, bytes::complete::tag};

use crate::{
    Error, IdentType, Result, SqlColumn, SupportedDBs,
    lexer::{Created, Lexer, parse_comment0},
};

pub type ColMap = IndexMap<String, SqlColumn>;
pub type TableMap = HashMap<String, SqlTable>;

#[allow(dead_code)]
pub struct SqlTable {
    pub columns: ColMap,
    pub primary_key: Option<String>,
}

pub struct SqlDB {
    pub tables: TableMap,
    pub db: SupportedDBs,
}

impl SqlDB {
    pub fn from_sql(db: SupportedDBs, statements: &str) -> Result<Self> {
        let mut tables = TableMap::new();

        let mut lexer = Lexer { db, statements };

        loop {
            loop {
                lexer.parser(parse_comment0)?;
                if !lexer.statements.starts_with("/*")
                    && !lexer.statements.starts_with("--")
                {
                    break;
                }
            }

            if lexer.statements.is_empty() {
                break;
            }

            let created = lexer.parse_statement()?;

            match created {
                Created::Table { name, columns, primary_key } => {
                    if tables.contains_key(&name) {
                        return Err(Error::DuplicateIdent(
                            name,
                            IdentType::Table,
                        ));
                    } else {
                        unsafe {
                            tables.insert_unique_unchecked(
                                name,
                                SqlTable { columns, primary_key },
                            )
                        };
                    }
                }

                Created::Index { table_name, columns, included } => {
                    let table =
                        tables.get_mut(table_name).ok_or_else(|| {
                            Error::MissingIdent(
                                table_name.to_string(),
                                IdentType::Table,
                            )
                        })?;

                    if let Some(ref included) = included {
                        for column in included {
                            if !table.columns.contains_key(column) {
                                return Err(Error::MissingIdent(
                                    column.clone(),
                                    IdentType::Column,
                                ));
                            }
                        }
                    }

                    for (col_name, mut index) in columns {
                        index.included_cols = included.clone();

                        table
                            .columns
                            .get_mut(col_name)
                            .ok_or_else(|| {
                                Error::MissingIdent(
                                    col_name.to_string(),
                                    IdentType::Column,
                                )
                            })?
                            .index = Some(index);
                    }
                }
            }

            lexer.parser(|remaining| {
                (parse_comment0, tag(";"), parse_comment0).parse(remaining)
            })?;

            if lexer.statements.is_empty() {
                break;
            }
        }

        Ok(Self { tables, db })
    }
}

#[cfg(test)]
mod tests {
    use crate::{SqlDB, SupportedDBs};

    #[test]
    fn test_comment_parsing() {
        let sql = r#"
        -- Comment

        -- Another one
        /* A block comment */
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Some notes on id
            name TEXT /* A multi-line
                        comment */
        ); -- One last comment for table

        CREATE UNIQUE -- Needs to be unique
        INDEX unique_id_index ON users /* Need an index for id */ USING btree (
            id, /* Most important
                  but also need to index name because it is sometimes accessed alone */
            name
        ); -- EOF
        "#;

        let db = SqlDB::from_sql(SupportedDBs::PostgreSQL, sql).unwrap();

        assert!(db.tables.contains_key("users"));
        assert!(
            db.tables.get("users").as_ref().unwrap().columns.contains_key("id")
        );
        assert!(
            db.tables
                .get("users")
                .as_ref()
                .unwrap()
                .columns
                .contains_key("name")
        );
    }
}
