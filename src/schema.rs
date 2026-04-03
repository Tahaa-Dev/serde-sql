use std::ops::{Index, IndexMut};

use crate::{
    Error, ErrorKind, IdentType, Result, SqlColumn, SupportedDBs,
    lexer::{Created, Lexer, parse_comment0},
};
use hashbrown::HashMap;
use indexmap::IndexMap;
use nom::{Offset, bytes::complete::tag};

#[derive(Default, Clone, Debug)]
pub struct ColMap {
    columns: IndexMap<String, SqlColumn>,
}

impl ColMap {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, key: String, value: SqlColumn) -> Option<SqlColumn> {
        self.columns.insert(key, value)
    }
    pub fn get(&self, key: &str) -> Option<&SqlColumn> {
        self.columns.get(key)
    }
    pub fn get_mut(&mut self, key: &str) -> Option<&mut SqlColumn> {
        self.columns.get_mut(key)
    }
    pub fn contains_key(&self, key: &str) -> bool {
        self.columns.contains_key(key)
    }
}

impl Index<&str> for ColMap {
    type Output = SqlColumn;

    fn index(&self, index: &str) -> &Self::Output {
        &self.columns[index]
    }
}

impl IndexMut<&str> for ColMap {
    fn index_mut(&mut self, index: &str) -> &mut Self::Output {
        &mut self.columns[index]
    }
}

#[derive(Default, Clone, Debug)]
pub struct SqlTable {
    pub columns: ColMap,
    #[allow(dead_code)]
    pub primary_key: Option<String>,
}

#[derive(Default, Clone, Debug)]
pub struct TableMap {
    tables: HashMap<String, SqlTable>,
}

impl TableMap {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, key: String, value: SqlTable) -> Option<SqlTable> {
        self.tables.insert(key, value)
    }
    pub fn get(&self, key: &str) -> Option<&SqlTable> {
        self.tables.get(key)
    }
    pub fn get_mut(&mut self, key: &str) -> Option<&mut SqlTable> {
        self.tables.get_mut(key)
    }
    pub fn contains_key(&self, key: &str) -> bool {
        self.tables.contains_key(key)
    }
}

impl Index<&str> for TableMap {
    type Output = SqlTable;

    fn index(&self, index: &str) -> &Self::Output {
        &self.tables[index]
    }
}

#[derive(Default, Clone, Debug)]
pub struct SqlDB {
    pub tables: TableMap,
    pub db: SupportedDBs,
}

impl SqlDB {
    pub fn from_sql(
        db: SupportedDBs,
        statements: impl AsRef<str>,
    ) -> Result<Self> {
        let statements = statements.as_ref();
        let mut tables = TableMap::new();

        let mut lexer = Lexer { db, statements, fks: vec![], orig: statements };

        loop {
            lexer.parser(parse_comment0)?;

            if lexer.statements.is_empty() {
                break;
            }

            let created = lexer.parse_statement()?;

            match created {
                Created::Table { name, columns, primary_key } => {
                    if tables
                        .insert(
                            name.to_string(),
                            SqlTable { columns, primary_key },
                        )
                        .is_some()
                    {
                        return Err(Error::new(
                            ErrorKind::DuplicateIdent(
                                name.to_string(),
                                IdentType::Table,
                            ),
                            statements.offset(name) + 1,
                        ));
                    }
                }

                Created::Index { table_name, columns, included, predicate } => {
                    let table =
                        tables.get_mut(table_name).ok_or_else(|| {
                            Error::new(
                                ErrorKind::MissingIdent(
                                    table_name.to_string(),
                                    IdentType::Table,
                                ),
                                statements.offset(table_name) + 1,
                            )
                        })?;

                    if let Some(ref included) = included {
                        let mut col = "";

                        if included.iter().any(|column| {
                            col = column;
                            !table.columns.contains_key(column)
                        }) {
                            return Err(Error::new(
                                ErrorKind::MissingIdent(
                                    col.to_string(),
                                    IdentType::Column,
                                ),
                                statements.offset(col) + 1,
                            ));
                        }
                    }

                    let included = included
                        .map(|v| v.iter().map(|s| s.to_string()).collect());
                    for (col_name, mut index) in columns {
                        index.included_cols = included.clone();
                        index.predicate = predicate.clone();

                        table
                            .columns
                            .get_mut(col_name)
                            .ok_or_else(|| {
                                Error::new(
                                    ErrorKind::MissingIdent(
                                        col_name.to_string(),
                                        IdentType::Column,
                                    ),
                                    statements.offset(col_name) + 1,
                                )
                            })?
                            .index = Some(index);
                    }
                }
            }

            lexer.parser((parse_comment0, tag(";"), parse_comment0))?;

            if lexer.statements.is_empty() {
                break;
            }
        }

        for (table, column) in lexer.fks {
            if let Some(table_cols) = tables.get(table) {
                if let Some(column) = column {
                    if !table_cols.columns.contains_key(column) {
                        return Err(Error::new(
                            ErrorKind::MissingIdent(
                                column.to_string(),
                                IdentType::Column,
                            ),
                            statements.offset(column) + 1,
                        ));
                    }
                } else if table_cols.primary_key.is_none() {
                    return Err(Error::new(
                        ErrorKind::MissingPrimaryKey(table.to_string()),
                        statements.offset(table) + 1,
                    ));
                }
            } else {
                return Err(Error::new(
                    ErrorKind::MissingIdent(
                        table.to_string(),
                        IdentType::Table,
                    ),
                    statements.offset(table) + 1,
                ));
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
