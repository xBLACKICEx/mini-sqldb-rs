use super::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::{Row, Value}};
use crate::error::{Error, Result};
use crate::sql::parser::ast::Expression;

mod kv;

/// Abstract SQL Engine definition, currently only KV Engine is supported
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

/// Abstract transaction information, including DDL and DML operations.
/// The underlying layer can accept ordinary KV storage engines, or access distributed storage engines.
pub trait Transaction {
    // Commit the transaction
    fn commit(&mut self) -> Result<()>;

    // Rollback the transaction
    fn rollback(&mut self) -> Result<()>;

    // DDL operations
    fn create_table(&mut self, table: Table) -> Result<()>;

    fn create_row(&mut self, table: String, row: Row) -> Result<()>;

    fn scan_table(&mut self, table_name: String, filter: Option<(String, Expression)>) -> Result<Vec<Row>>;

    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()>;

    fn delete_row(&mut self, table: &Table, id: Value) -> Result<()>;

    // Get table info
    fn get_table(&mut self, table_name: &str) -> Result<Option<Table>>;

    fn must_get_table(&mut self, table_name: &str) -> Result<Table> {
        self.get_table(table_name)?
            .ok_or(Error::InternalError(format!(
                "table {table_name} does not exist"
            )))
    }
}

/// Client SQL Session definition
pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine + 'static> Session<E> {
    /// Execute client SQL statements
    pub fn execute(&self, sql: &str) -> Result<ResultSet> {
        let stmt = Parser::new(sql).parse()?;
        let mut txn = self.engine.begin()?;

        // Build plan and execute SQL statement
        match Plan::build(stmt).execute(&mut txn) {
            Ok(rs) => {
                txn.commit()?;
                Ok(rs)
            }
            Err(e) => {
                txn.rollback()?;
                Err(e)
            }
        }
    }
}
