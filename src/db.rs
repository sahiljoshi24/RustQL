use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sqlparser::ast::Expr;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ValueType {
    Int(i32),
    Float(f64),
    String(String),
    Null,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ValueType>>,
}

#[derive(Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<String>) {
        self.tables.insert(
            name.to_string(),
            Table {
                name: name.to_string(),
                columns,
                rows: Vec::new(),
            },
        );
    }

    pub fn insert_into(&mut self, table_name: &str, values: Vec<String>) -> Result<(), String> {
        match self.tables.get_mut(table_name) {
            Some(table) => {
                if values.len() != table.columns.len() {
                    return Err("Column count mismatch".to_string());
                }
                let value_types = values
                    .into_iter()
                    .map(|v| ValueType::String(v))
                    .collect();
                table.rows.push(value_types);
                Ok(())
            }
            None => Err("Table not found".to_string()),
        }
    }

    pub fn select_from(&self, table_name: &str) -> Result<&Table, String> {
        self.tables
            .get(table_name)
            .ok_or("Table not found".to_string())
    }

    pub fn select_with_condition(
        &self,
        table_name: &str,
        condition: &Expr,
    ) -> Result<Vec<Vec<String>>, String> {
        let table = self.select_from(table_name)?;

        let condition_fn = |row: &Vec<ValueType>| -> bool {
            if let Expr::BinaryOp { left, right, .. } = condition {
                if let Expr::Value(Value::Number(num, _)) = **right {
                    if let Some(ValueType::Int(val)) = row.get(0) {
                        return val.to_string() == num;
                    }
                }
            }
            false
        };

        let filtered_rows: Vec<Vec<String>> = table
            .rows
            .iter()
            .filter(|row| condition_fn(row))
            .map(|row| row.iter().map(|v| format!("{:?}", v)).collect())
            .collect();

        Ok(filtered_rows)
    }

    pub fn update_rows(
        &mut self,
        table_name: &str,
        assignments: Vec<SetAssignment>,
        condition: Expr,
    ) -> Result<usize, String> {
        let table = self.tables.get_mut(table_name).ok_or("Table not found")?;

        let updated_count = table
            .rows
            .iter_mut()
            .filter(|row| self.matches_condition(row, &condition))
            .map(|row| {
                if let Some(assign) = assignments.first() {
                    if let Expr::Value(Value::SingleQuotedString(new_value)) = &assign.value {
                        row[0] = ValueType::String(new_value.clone());
                    }
                }
            })
            .count();

        Ok(updated_count)
    }

    pub fn delete_rows(&mut self, table_name: &str, condition: Expr) -> Result<usize, String> {
        let table = self.tables.get_mut(table_name).ok_or("Table not found")?;

        let initial_len = table.rows.len();
        table.rows.retain(|row| !self.matches_condition(row, &condition));

        let deleted_count = initial_len - table.rows.len();
        Ok(deleted_count)
    }

    fn matches_condition(&self, row: &Vec<ValueType>, condition: &Expr) -> bool {
        if let Expr::BinaryOp { left, right, .. } = condition {
            if let Expr::Value(Value::Number(num, _)) = **right {
                if let Some(ValueType::Int(val)) = row.get(0) {
                    return val.to_string() == num;
                }
            }
        }
        false
    }
}
