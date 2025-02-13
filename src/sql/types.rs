use crate::sql::parser::ast::Expression;

#[derive(Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Float,
    Integer,
    String,
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl From<Expression> for Value {
    fn from(expr: Expression) -> Self {
        match expr {
            Expression::Consts(consts) => match consts {
                crate::sql::parser::ast::Consts::Null => Value::Null,
                crate::sql::parser::ast::Consts::Boolean(b) => Value::Boolean(b),
                crate::sql::parser::ast::Consts::Integer(i) => Value::Integer(i),
                crate::sql::parser::ast::Consts::String(s) => Value::String(s),
                crate::sql::parser::ast::Consts::Float(f) => Value::Float(f),
            },
        }
    }
}