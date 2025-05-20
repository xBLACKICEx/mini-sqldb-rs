use super::{
    engine::Transaction,
    executor::{Executor, ResultSet},
    parser::ast::OrderDirection,
};
use crate::error::Result;
use crate::sql::{parser::ast, parser::ast::Expression, plan::planner::Planner, schema::Table};
use std::collections::BTreeMap;

mod planner;

/// Execution Node
#[derive(Debug, PartialEq)]
pub enum Node {
    // Create Table
    CreateTable {
        schema: Table,
    },

    // Insert Data
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },

    // Scan Node
    Scan {
        table_name: String,
        filter: Option<(String, Expression)>,
    },

    // Update Node
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        source: Box<Node>,
    },

    // Delete Node
    Delete {
        table_name: String,
        source: Box<Node>,
    },

    // Order Node
    Order {
        order_by: Vec<(String, OrderDirection)>,
        source: Box<Node>,
    },

    Limit {
        source: Box<Node>,
        limit: usize
    },

    Offset {
        source: Box<Node>,
        offset: usize
    },
}

#[derive(Debug, PartialEq)]
/// Execution Plan Definition, the bottom layer is different types of execution nodes
pub struct Plan(pub Node);

impl Plan {
    pub fn build(stmt: ast::Statement) -> Result<Self> {
        Planner::new().build(stmt)
    }

    pub fn execute<T: Transaction + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.0).execute(txn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::Parser;
    use crate::sql::schema::Column;
    use crate::sql::types::{DataType, Value};
    #[test]
    fn test_plan_create_table() -> Result<()> {
        let sql = "
    CREATE TABLE tbl1 (
        id INT PRIMARY KEY,
        a INT DEFAULT 100,
        b FLOAT NOT NULL,
        c VARCHAR,
        d BOOL DEFAULT true
    );
";
        let stmt = Parser::new(sql).parse()?;
        let plan = Plan::build(stmt)?;
        assert_eq!(
            plan,
            Plan(Node::CreateTable {
                schema: Table {
                    name: "tbl1".to_string(),
                    columns: vec![
                        Column {
                            name: "id".to_string(),
                            datatype: DataType::Integer,
                            nullable: false,
                            default: None,
                            primary_key: true,
                        },
                        Column {
                            name: "a".to_string(),
                            datatype: DataType::Integer,
                            nullable: true, // If NOT NULL is not specified, it defaults to allowing null
                            default: Some(Value::Integer(100)),
                            primary_key: false,
                        },
                        Column {
                            name: "b".to_string(),
                            datatype: DataType::Float,
                            nullable: false,
                            default: None,
                            primary_key: false,
                        },
                        Column {
                            name: "c".to_string(),
                            datatype: DataType::String,
                            nullable: true,
                            default: Some(Value::Null),
                            primary_key: false,
                        },
                        Column {
                            name: "d".to_string(),
                            datatype: DataType::Boolean,
                            nullable: true,
                            default: Some(Value::Boolean(true)),
                            primary_key: false,
                        },
                    ]
                }
            })
        );

        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        // 1) Single row insertion without column names
        let sql1 = "INSERT INTO tbl1 VALUES (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1)?;
        assert_eq!(
            p1,
            Plan(Node::Insert {
                table_name: "tbl1".to_string(),
                columns: vec![],
                values: vec![vec![
                    Expression::Consts(ast::Consts::Integer(1)),
                    Expression::Consts(ast::Consts::Integer(2)),
                    Expression::Consts(ast::Consts::Integer(3)),
                    Expression::Consts(ast::Consts::String("a".to_string())),
                    Expression::Consts(ast::Consts::Boolean(true)),
                ]],
            })
        );

        // 2) Multi-row insertion with column names
        let sql2 = "INSERT INTO tbl2 (c1, c2, c3) VALUES (3, 'a', true), (4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2)?;
        assert_eq!(
            p2,
            Plan(Node::Insert {
                table_name: "tbl2".to_string(),
                columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
                values: vec![
                    vec![
                        Expression::Consts(ast::Consts::Integer(3)),
                        Expression::Consts(ast::Consts::String("a".to_string())),
                        Expression::Consts(ast::Consts::Boolean(true)),
                    ],
                    vec![
                        Expression::Consts(ast::Consts::Integer(4)),
                        Expression::Consts(ast::Consts::String("b".to_string())),
                        Expression::Consts(ast::Consts::Boolean(false)),
                    ],
                ],
            })
        );
        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let sql = "SELECT * FROM tbl1;";
        let stmt = Parser::new(sql).parse()?;
        let plan = Plan::build(stmt)?;
        assert_eq!(
            plan,
            Plan(Node::Scan {
                table_name: "tbl1".to_string(),
                filter: None
            })
        );
        Ok(())
    }
}
