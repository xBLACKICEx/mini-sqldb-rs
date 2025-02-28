use super::{Executor, ResultSet};
use crate::{
    error::Result,
    sql::{engine::Transaction, schema::Table},
};

// Crate table
pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table_name = self.schema.name.clone();
        txn.create_table(self.schema)?;

        Ok(ResultSet::CreateTable { table_name })
    }
}
