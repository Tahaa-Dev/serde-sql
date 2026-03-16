use serde_sql::{IndexMethod, SqlDB, SqlType, SupportedDBs, error::Error};

#[test]
fn test_valid() -> Result<(), Error> {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Primary key with default since it is unique
            name TEXT,
            email TEXT
        );

        CREATE INDEX ON users USING hash (id) INCLUDE (email, name) WITH (fillfactor=90 /* need 90 fillfactor */);"#;

    let db = SqlDB::from_sql(SupportedDBs::PostgreSQL, sql)?;
    let db = &db.tables.get("users").unwrap();

    assert!(db.columns.contains_key("id"));
    assert!(db.columns.contains_key("name"));
    assert!(db.columns.contains_key("email"));
    assert_eq!(db.columns.get("id").unwrap().sql_type, SqlType::Uuid);
    assert_eq!(db.columns.get("name").unwrap().sql_type, SqlType::Text);
    assert_eq!(db.columns.get("email").unwrap().sql_type, SqlType::Text);
    assert!({
        let m = &db.columns.get("id").as_ref().unwrap().index.as_ref().unwrap();
        if let Some(IndexMethod::Hash { fillfactor }) = &m.method {
            fillfactor.is_none() && m.name.is_none()
        } else {
            false
        }
    });
    assert_eq!(db.primary_key.as_ref().unwrap(), "id");

    Ok(())
}
