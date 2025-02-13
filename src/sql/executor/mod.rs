use crate::error::Result;
use crate::sql::{plan::Node, types::Row};
use mutation::Insert;
use query::Scan;
use schema::CreateTable;

mod mutation;
mod query;
mod schema;

pub trait Executor {
    fn execute(&self) -> Result<ResultSet>;
}

impl dyn Executor {
    pub fn build(node: Node) -> Box<dyn Executor> {
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
