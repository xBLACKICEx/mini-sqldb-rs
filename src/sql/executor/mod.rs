use super::{engine::Transaction, plan::Node, types::Row};
use crate::error::Result;
use mutation::Insert;
use query::Scan;
use schema::CreateTable;

mod mutation;
mod query;
mod schema;

pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: Transaction> dyn Executor<T> {
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}

#[derive(Debug)]
pub enum ResultSet {
    CrateTable {
        table_name: String,
    },
    Insert {
        count: usize,
    },
    Scan {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
}
