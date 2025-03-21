use crate::db::Database;
use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Statement, TableFactor, Value, Values};
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
                if let Query { body, .. } = *source {
                    if let SetExpr::Values(Values { rows, .. }) = *body {
                        for row in rows {
                            let vals: Vec<String> = row
                                .iter()
                                .map(|val| match val {
                                    Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                                    Expr::Value(Value::Number(n, _)) => n.clone(), // Handle numbers
                                    _ => "NULL".to_string(),
                                })
                                .collect();

                            db.insert_into(&table_name.to_string(), vals)?;
                        }
                        return Ok("Row inserted".to_string());
                    }
                }
            }

            Statement::Query(query) => {
                if let SetExpr::Select(select) = &*query.body {
                    match &select.projection[..] {
                        [SelectItem::Wildcard(_)] => {
                            if let Some(from_table) = select.from.first() {
                                if let TableFactor::Table { name, .. } = &from_table.relation {
                                    let table_name = name.to_string();
                                    if let Ok(table_data) = db.select_from(&table_name) {
                                        let result = serde_json::to_string(&table_data).unwrap();
                                        return Ok(result);
                                    }
                                }
                            }
                        }
                        _ => return Err("Only SELECT * is supported".to_string()),
                    }
                }
            }

            _ => return Err("Unsupported query".to_string()),
        }
    }

    Err("Invalid query".to_string())
}
