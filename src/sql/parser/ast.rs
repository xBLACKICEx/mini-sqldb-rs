use std::collections::BTreeMap;

use crate::sql::types::DataType;

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Select {
        table_name: String,
        where_clause: Option<(String, Expression)>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },

    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<(String, Expression)>,
    },

    Delete {
        table_name: String,
        where_clause: Option<(String, Expression)>,
    },
}


#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Consts(Consts),
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    Integer(i64),
    String(String),
    Boolean(bool),
    Float(f64),
}
