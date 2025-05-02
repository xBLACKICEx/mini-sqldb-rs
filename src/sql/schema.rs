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
            1 => {}
            0 => {
                return Err(Error::InternalError(format!(
                    "Table {} has no primary key",
                    self.name
                )))
            }
            _ => {
                return Err(Error::InternalError(format!(
                    "Table {} has more than one primary key",
                    self.name
                )))
            }
        }

        for col in &self.columns {
            // primary key must be not null
            if col.primary_key && col.nullable {
                return Err(Error::InternalError(format!(
                    "Column {} in table {} is primary key but nullable",
                    col.name, self.name
                )));
            }

            // check if default value is valid
            if let Some(default) = &col.default {
                match default.datatype() {
                    Some(dt) if dt != col.datatype => {
                        return Err(Error::InternalError(format!(
                            "Column {} in table {} has default value {} but datatype is {:?}",
                            col.name, self.name, default, col.datatype
                        )))
                    }
                    _ => {}
                }
            }
        }

        Ok(())
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
