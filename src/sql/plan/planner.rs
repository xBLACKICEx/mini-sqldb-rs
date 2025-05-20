use super::{Node, Plan};
use crate::{
    error::{Error, Result},
    sql::{
        parser::ast,
        schema::{self, Table},
        types::Value,
    },
};

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&mut self, stmt: ast::Statement) -> Result<Plan> {
        Ok(Plan(self.build_statement(stmt)?))
    }

    fn build_statement(&mut self, stmt: ast::Statement) -> Result<Node> {
        Ok(match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.primary_key);
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
            ast::Statement::Select {
                table_name,
                where_clause,
                order_by,
                limit,
                offset,
            } => {
                let mut node = Node::Scan {
                    table_name,
                    filter: where_clause,
                };

                if !order_by.is_empty() {
                    node = Node::Order {
                        order_by,
                        source: Box::new(node),
                    }
                }
                // TODO: limit/offset are constrained by Value::Integer i64 need to be usize
                if let Some(offset) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: match Value::from(&offset) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::InternalError(format!("invald offset"))),
                        },
                    }
                }

                if let Some(limit) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: match Value::from(&limit) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::InternalError(format!("invald limit"))),
                        },
                    }
                }

                node
            }
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
            ast::Statement::Delete {
                table_name,
                where_clause,
            } => Node::Delete {
                table_name: table_name.clone(),
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
            },
        })
    }
}
