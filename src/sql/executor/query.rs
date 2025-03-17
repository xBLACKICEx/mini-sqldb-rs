use super::{Executor, ResultSet};
use crate::{error::Result, sql::{engine::Transaction, parser::ast::Expression}};

pub struct Scan {
    table_name: String,
    filter: Option<(String, Expression)>,
}

impl Scan {
    pub fn new(table_name: String, filter: Option<(String, Expression)>) -> Box<Self> {
        Box::new(Self { table_name, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_get_table(&self.table_name)?;
        let rows = txn.scan_table(self.table_name.clone(), self.filter)?;

        Ok(ResultSet::Scan {
            columns: table.columns.iter().map(|c| c.name.clone()).collect(),
            rows,
        })
    }
}
