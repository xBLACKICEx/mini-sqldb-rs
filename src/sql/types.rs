use crate::sql::parser::ast::Expression;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Float,
    Integer,
    String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }
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

pub type Row = Vec<Value>;
