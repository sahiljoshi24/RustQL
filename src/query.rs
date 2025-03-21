use crate::db::Database;
use sqlparser::ast::{Expr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn execute_sql(db: &mut Database, query: &str) -> Result<String, String> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, query).map_err(|e| e.to_string())?;

    for statement in ast {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let table_name = name.to_string();
                let col_names = columns.iter().map(|col| col.name.to_string()).collect();
                db.create_table(&table_name, col_names);
                return Ok(format!("Table '{}' created", table_name));
            }
            Statement::Insert {
                table_name, source, ..
            } => {
                if let Some(Statement::Values(values)) = source.as_ref().map(|v| v.as_ref()) {
                    let vals: Vec<String> = values.rows[0]
                        .iter()
                        .map(|val| match val {
                            Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                            _ => "NULL".to_string(),
                        })
                        .collect();

                    db.insert_into(&table_name.to_string(), vals)?;
                    return Ok("Row inserted".to_string());
                }
            }
            Statement::Query(query) => {
                if let Some(table) = query.body.as_table() {
                    let table_name = table.name.to_string();
                    if let Ok(table_data) = db.select_from(&table_name) {
                        let result = serde_json::to_string(&table_data).unwrap();
                        return Ok(result);
                    }
                }
            }
            _ => return Err("Unsupported query".to_string()),
        }
    }

    Err("Invalid query".to_string())
}
