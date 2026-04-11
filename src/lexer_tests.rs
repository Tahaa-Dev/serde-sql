#![cfg(test)]
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
    let (rem, c) = Lexer::pg_parse_inline_fk("REFERENCES users rest").unwrap();
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
    let (rem, c) = Lexer::pg_parse_fkaction("ON DELETE CASCADE rest").unwrap();
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
    let (rem, c) = Lexer::pg_parse_fkaction("ON UPDATE RESTRICT rest").unwrap();
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
    let (rem, c) = Lexer::pg_parse_fkaction("ON DELETE SET NULL rest").unwrap();
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
    let (rem, (ty, _)) = Lexer::pg_parse_type("DOUBLE PRECISION rest").unwrap();
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
    let (rem, (ty, args)) = Lexer::pg_parse_type("VARCHAR(255) rest").unwrap();
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
    let (rem, (ty, _)) = Lexer::pg_parse_type("INTERVAL YEAR rest").unwrap();
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
    assert!(matches!(Lexer::match_type("INT2".into(), &[]), SqlType::SmallInt));
    assert!(matches!(
        Lexer::match_type("INTEGER".into(), &[]),
        SqlType::Integer
    ));
    assert!(matches!(Lexer::match_type("INT".into(), &[]), SqlType::Integer));
    assert!(matches!(Lexer::match_type("INT4".into(), &[]), SqlType::Integer));
    assert!(matches!(Lexer::match_type("BIGINT".into(), &[]), SqlType::BigInt));
    assert!(matches!(Lexer::match_type("INT8".into(), &[]), SqlType::BigInt));
}

#[test]
fn match_type_floats() {
    assert!(matches!(Lexer::match_type("REAL".into(), &[]), SqlType::Real));
    assert!(matches!(Lexer::match_type("FLOAT4".into(), &[]), SqlType::Real));
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
    assert!(matches!(Lexer::match_type("CHAR".into(), &[]), SqlType::Char(1)));
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
    assert!(matches!(Lexer::match_type("BYTEA".into(), &[]), SqlType::ByteA));
    assert!(matches!(
        Lexer::match_type("BOOLEAN".into(), &[]),
        SqlType::Boolean
    ));
    assert!(matches!(Lexer::match_type("BOOL".into(), &[]), SqlType::Boolean));
    assert!(matches!(Lexer::match_type("INET".into(), &[]), SqlType::Inet));
    assert!(matches!(Lexer::match_type("CIDR".into(), &[]), SqlType::Cidr));
    assert!(matches!(
        Lexer::match_type("MACADDR".into(), &[]),
        SqlType::MacAddr
    ));
    assert!(matches!(Lexer::match_type("JSON".into(), &[]), SqlType::Json));
    assert!(matches!(Lexer::match_type("JSONB".into(), &[]), SqlType::Jsonb));
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
    assert!(matches!(Lexer::match_type("SERIAL".into(), &[]), SqlType::Serial));
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
    let (rem, items) =
        Lexer::parse_list(Lexer::parse_ident).parse("( col1 , col2 )").unwrap();
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
    let input = "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE";
    let mut fks: Vec<(&str, Option<&str>)> = vec![];
    let (rem, iter) = Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
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
    let (rem, iter) = Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
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
    let (rem, iter) = Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
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
    let (rem, iter) = Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
    let results: Vec<_> = iter.collect();
    assert_eq!(results[0].0.on_update, Some(FkAction::Restrict));
    assert_eq!(results[0].0.on_delete, None);
    assert_eq!(rem, "");
}

#[test]
fn table_level_fk_with_both_actions() {
    let input = "FOREIGN KEY (col) REFERENCES t(c) ON DELETE SET NULL ON UPDATE CASCADE";
    let mut fks: Vec<(&str, Option<&str>)> = vec![];
    let (rem, iter) = Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
    let results: Vec<_> = iter.collect();
    let fk = &results[0].0;
    assert_eq!(fk.on_delete, Some(FkAction::SetNull));
    assert_eq!(fk.on_update, Some(FkAction::Cascade));
    assert_eq!(rem, "");
}

#[test]
fn table_level_fk_with_comments() {
    let input = "FOREIGN KEY /* fk */ (user_id) -- note\n REFERENCES users(id)";
    let mut fks: Vec<(&str, Option<&str>)> = vec![];
    let (rem, iter) = Lexer::pg_parse_table_level_fk(input, &mut fks).unwrap();
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
fn pg_parse_index_method_none() {
    let (rem, method) = Lexer::pg_parse_index_method("col1, col2").unwrap();
    assert!(method.is_none());
    assert_eq!(rem, "col1, col2");
}

#[test]
fn pg_parse_index_method_some() {
    let (rem, method) =
        Lexer::pg_parse_index_method("USING HASH (col1)").unwrap();
    assert_eq!(method.unwrap(), "HASH");
    assert_eq!(rem, " (col1)");
}

#[test]
fn match_idx_method_valid() {
    let (_, method) = Lexer::match_idx_method(Some("BTREE")).unwrap();
    assert!(matches!(method.unwrap(), crate::IndexMethod::BTree { .. }));
    let (_, method) = Lexer::match_idx_method(Some("GIN")).unwrap();
    assert!(matches!(method.unwrap(), crate::IndexMethod::Gin { .. }));
}

#[test]
fn match_idx_method_invalid() {
    assert!(Lexer::match_idx_method(Some("INVALID")).is_err());
}

#[test]
fn pg_parse_with_params_present() {
    let (rem, params) =
        Lexer::pg_parse_with_params(" WITH (fillfactor = 70, buffering = ON)")
            .unwrap();
    let params = params.unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], ("fillfactor", "70"));
    assert_eq!(params[1], ("buffering", "ON"));
    assert!(rem.is_empty());
}

#[test]
fn pg_apply_with_params_valid() {
    let mut method = Some(crate::IndexMethod::BTree { fillfactor: None });
    let params = Some(vec![("fillfactor", "80")]);
    Lexer::pg_apply_with_params(&mut method, params, 0).unwrap();
    if let crate::IndexMethod::BTree { fillfactor } = method.unwrap() {
        assert_eq!(fillfactor, Some(80));
    } else {
        panic!("expected BTree");
    }
}
