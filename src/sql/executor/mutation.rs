use super::{Executor, ResultSet};
use crate::{
    error::Result,
    sql::{engine::Transaction, parser::ast::Expression},
};

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(&self, txn: &mut T) -> Result<ResultSet> {
        todo!()
    }
}
