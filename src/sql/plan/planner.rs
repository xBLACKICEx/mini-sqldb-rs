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
        todo!()
    }
}
