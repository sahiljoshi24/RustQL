use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
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
                table.rows.push(values);
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
}
