use super::{Executor, ResultSet};
use crate::error::Error;
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::{
    error::Result,
    sql::{engine::Transaction, parser::ast::Expression},
};
use std::collections::HashMap;

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

// Column alignment
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c          d
// 1       2       3      default fill
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();

    for column in table.columns.iter().skip(row.len()) {
        if let Some(default_value) = &column.default {
            results.push(default_value.clone());
        } else {
            return Err(Error::InternalError(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }

    Ok(results)
}

// tbl:
// insert into tbl(d, c) values(1, 2);
//    a          b       c          d
// default   default     2          1
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // Determine if the number of columns is consistent with the number of values
    if columns.len() != values.len() {
        return Err(Error::InternalError(format!(
            "Columns count {} does not match values count {}",
            columns.len(),
            values.len()
        )));
    }

    let input_map = columns.iter().zip(values.iter()).collect::<HashMap<_, _>>();

    table
        .columns
        .iter()
        .map(|column| {
            if let Some(value) = input_map.get(&column.name) {
                Ok((*value).clone())
            } else if let Some(default_value) = &column.default {
                Ok(default_value.clone())
            } else {
                Err(Error::InternalError(format!(
                    "No default value for column {}",
                    column.name
                )))
            }
        })
        .collect()
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        // First, retrieve the table information
        let table = txn.must_get_table(&self.table_name)?;
        for expr in self.values {
            // Convert the expression into a value
            let row = expr.into_iter().map(|e| Value::from(e)).collect::<Vec<_>>();
            // If the inserted column is not specified
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                // If the inserted column is specified, the value information needs to be organized
                make_row(&table, &self.columns, &row)?
            };

            // Insert data
            println!("insert row: {:?}", insert_row);
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }

        Ok(ResultSet::Insert { count })
    }
}
