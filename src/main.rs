mod db;
mod query;

use db::Database;
use query::execute_sql;
use std::io::{self, Write};

fn main() {
    let mut db = Database::new();
    loop {
        print!("SQL> ");
        io::stdout().flush().unwrap();

        let mut query = String::new();
        io::stdin().read_line(&mut query).unwrap();
        let query = query.trim();

        if query.eq_ignore_ascii_case("exit") {
            break;
        }

        match execute_sql(&mut db, query) {
            Ok(result) => println!("{}", result),
            Err(err) => eprintln!("Error: {}", err),
        }
    }
}
