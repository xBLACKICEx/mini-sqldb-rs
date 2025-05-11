use crate::sql::parser::ast::Expression;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::{Display, Formatter}};

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

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(b) => {
                if *b { write!(f, "TRUE") } else { write!(f, "FALSE") }
            }
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::String(s) => write!(f, "'{}'", s),
        }
    }
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

impl From<&Expression> for Value {
    fn from(expr: &Expression) -> Self {
        match expr {
            Expression::Consts(consts) => match consts {
                crate::sql::parser::ast::Consts::Null => Value::Null,
                crate::sql::parser::ast::Consts::Boolean(b) => Value::Boolean(*b),
                crate::sql::parser::ast::Consts::Integer(i) => Value::Integer(*i),
                crate::sql::parser::ast::Consts::String(s) => Value::String(s.clone()),
                crate::sql::parser::ast::Consts::Float(f) => Value::Float(*f),
            },
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (_, _) => None
        }
    }
}

pub type Row = Vec<Value>;
