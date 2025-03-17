use super::{Node, Plan};
use crate::sql::parser::ast;
use crate::sql::schema::{self, Table};
use crate::sql::types::Value;

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&mut self, stmt: ast::Statement) -> Plan {
        Plan(self.build_statement(stmt))
    }

    fn build_statement(&mut self, stmt: ast::Statement) -> Node {
        match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(true);
                            let default = match c.default {
                                Some(express) => Some(Value::from(&express)),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };

                            schema::Column {
                                name: c.name,
                                datatype: c.data_type,
                                nullable,
                                default,
                                primary_key: c.primary_key,
                            }
                        })
                        .collect(),
                },
            },
            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => Node::Insert {
                table_name,
                values,
                columns: columns.unwrap_or_default(),
            },
            ast::Statement::Select { table_name } => Node::Scan {
                table_name,
                filter: None,
            },
            ast::Statement::Update {
                table_name,
                columns,
                where_clause,
            } => Node::Update {
                table_name: table_name.clone(),
                columns,
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
            },
        }
    }
}
