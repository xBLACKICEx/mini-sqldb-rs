use serde::{Deserialize, Serialize};

use super::types::{DataType, Row, Value};
use crate::error::{Error, Result};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn is_validate(&self) -> Result<()> {
        // Validate the table
        if self.columns.is_empty() {
            return Err(Error::InternalError(format!(
                "Table {} has no columns",
                self.name
            )));
        }

        // Check if primary key is more than one
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => Ok(()),
            0 => Err(Error::InternalError(format!(
                "Table {} has no primary key",
                self.name
            ))),
            _ => Err(Error::InternalError(format!(
                "Table {} has more than one primary key",
                self.name
            ))),
        }
    }

    pub fn get_primary_key<'a>(&self, row: &'a Row) -> Result<&'a Value> {
        let col = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .expect(format!("Table {} has no primary key", self.name).as_str());

        Ok(&row[col])
    }

    pub fn get_col_index(&self, col_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or(Error::InternalError(format!(
                "column {} not found",
                col_name
            )))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
}
