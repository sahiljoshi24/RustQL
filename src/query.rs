use crate::db::Database;
use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Statement, TableFactor, Value, Values, Ident, SetAssignment};
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
                                    Expr::Value(Value::Number(n, _)) => n.clone(),
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

                    if let Some(where_clause) = &select.selection {
                        let filtered_rows = db.select_with_condition(&select.from[0].to_string(), where_clause);
                        match filtered_rows {
                            Ok(rows) => return Ok(serde_json::to_string(&rows).unwrap()),
                            Err(err) => return Err(err),
                        }
                    }
                }
            }

            Statement::Update { table_name, assignments, selection, .. } => {
                if let Some(selection_expr) = selection {
                    let updated_rows = db.update_rows(&table_name.to_string(), assignments, selection_expr);
                    match updated_rows {
                        Ok(updated_count) => return Ok(format!("{} rows updated", updated_count)),
                        Err(err) => return Err(err),
                    }
                } else {
                    return Err("No WHERE condition specified for UPDATE".to_string());
                }
            }

            Statement::Delete { table_name, selection, .. } => {
                if let Some(selection_expr) = selection {
                    let deleted_rows = db.delete_rows(&table_name.to_string(), selection_expr);
                    match deleted_rows {
                        Ok(deleted_count) => return Ok(format!("{} rows deleted", deleted_count)),
                        Err(err) => return Err(err),
                    }
                } else {
                    return Err("No WHERE condition specified for DELETE".to_string());
                }
            }

            _ => return Err("Unsupported query".to_string()),
        }
    }

    Err("Invalid query".to_string())
}
