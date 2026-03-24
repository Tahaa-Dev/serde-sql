use serde_sql::{ForeignKey, IndexMethod, SqlDB, SqlType, SupportedDBs};

#[test]
fn test_valid() {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Primary key with default since it is unique
            name TEXT,
            email TEXT,
            total_purchases DECIMAL(10, 2) CHECK (total_purchases > 0.00 AND (total_purchases < 9999999999.00))
        );
        
        CREATE TABLE IF NOT EXISTS purchases (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users,
            user_email TEXT REFERENCES users(email),
            username TEXT,
            name TEXT,
            amount DECIMAL(4, 2), -- Most expensive item is $1299.99
            FOREIGN KEY (username) REFERENCES users(name)
        );

        CREATE INDEX ON users USING hash (id) INCLUDE (email, name) WITH (fillfactor=90 /* need 90 fillfactor */) WHERE total_purchases > 1000.00 ;"#;

    let database = SqlDB::from_sql(SupportedDBs::PostgreSQL, sql).unwrap();
    let db = &database.tables["users"];
    let db2 = &database.tables["purchases"];

    assert_eq!(db.columns["id"].sql_type, SqlType::Uuid);
    assert_eq!(db.columns["name"].sql_type, SqlType::Text);
    assert_eq!(db.columns["email"].sql_type, SqlType::Text);

    assert_eq!(
        db.columns["total_purchases"].sql_type,
        SqlType::Decimal(Some(10), Some(2))
    );

    assert!({
        let idx = &db.columns["id"].index.as_ref().unwrap();
        if let Some(IndexMethod::Hash { fillfactor }) = &idx.method {
            fillfactor.is_none()
                && idx.name.is_none()
                && idx.predicate.as_deref() == Some("total_purchases > 1000.00")
        } else {
            false
        }
    });

    assert_eq!(db.primary_key.as_deref(), Some("id"));
    assert_eq!(db.columns["id"].default.as_deref(), Some("gen_random_uuid()"));
    assert_eq!(
        db.columns["total_purchases"].check.as_deref(),
        Some("total_purchases > 0.00 AND (total_purchases < 9999999999.00)")
    );

    assert_eq!(
        db2.columns["user_id"].foreign_key,
        Some(ForeignKey { table: "users".to_string(), column: None })
    );
    assert_eq!(
        db2.columns["user_email"].foreign_key,
        Some(ForeignKey {
            table: "users".to_string(),
            column: Some("email".to_string())
        })
    );
    assert_eq!(
        db2.columns["username"].foreign_key,
        Some(ForeignKey {
            table: "users".to_string(),
            column: Some("name".to_string())
        })
    );
}

#[test]
fn test_invalid() {
    let res = SqlDB::from_sql(
        SupportedDBs::PostgreSQL,
        r#"CREATE table should_fail ON users WITH"#,
    );

    assert_eq!(
        unsafe { res.unwrap_err_unchecked() },
        serde_sql::error::Error::UnexpectedToken(
            "ON users WITH".into(),
            "<KEYWORD>".into()
        )
    );
}

#[test]
fn test_invalid_fk_missing_table() {
    let res = SqlDB::from_sql(
        SupportedDBs::PostgreSQL,
        "CREATE TABLE IF NOT EXISTS purchases (user_id UUID PRIMARY KEY REFERENCES users(id));",
    );

    assert_eq!(
        unsafe { res.unwrap_err_unchecked() },
        serde_sql::error::Error::MissingIdent(
            "users".to_string(),
            serde_sql::error::IdentType::Table
        )
    )
}

#[test]
fn test_invalid_fk_missing_pk() {
    let res = SqlDB::from_sql(
        SupportedDBs::PostgreSQL,
        r#"CREATE TABLE IF NOT EXISTS users (id UUID);
        CREATE TABLE IF NOT EXISTS purchases (user_id UUID PRIMARY KEY REFERENCES users);"#,
    );

    assert_eq!(
        unsafe { res.unwrap_err_unchecked() },
        serde_sql::error::Error::MissingIdent(
            "PRIMARY KEY FOR TABLE: users".to_string(),
            serde_sql::error::IdentType::Column
        )
    )
}

#[test]
fn test_invalid_fk_missing_col() {
    let res = SqlDB::from_sql(
        SupportedDBs::PostgreSQL,
        r#"CREATE TABLE IF NOT EXISTS users (name TEXT);
        CREATE TABLE IF NOT EXISTS purchases (user_id UUID PRIMARY KEY REFERENCES users(id));"#,
    );

    assert_eq!(
        unsafe { res.unwrap_err_unchecked() },
        serde_sql::error::Error::MissingIdent(
            "id".to_string(),
            serde_sql::error::IdentType::Column
        )
    )
}

#[test]
fn test_invalid_table_fk_missing_col() {
    let res = SqlDB::from_sql(
        SupportedDBs::PostgreSQL,
        r#"CREATE TABLE IF NOT EXISTS users (name TEXT);
        CREATE TABLE IF NOT EXISTS purchases (user_id UUID PRIMARY KEY, FOREIGN KEY (user_id) REFERENCES users(id));"#,
    );

    assert_eq!(
        unsafe { res.unwrap_err_unchecked() },
        serde_sql::error::Error::MissingIdent(
            "id".to_string(),
            serde_sql::error::IdentType::Column
        )
    )
}
