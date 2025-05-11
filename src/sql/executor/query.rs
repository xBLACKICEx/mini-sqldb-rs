use std::{cmp::Ordering, collections::HashMap};

use super::{Executor, ResultSet};
use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        parser::ast::{Expression, OrderDirection},
    },
};

pub struct Scan {
    table_name: String,
    filter: Option<(String, Expression)>,
}

impl Scan {
    pub fn new(table_name: String, filter: Option<(String, Expression)>) -> Box<Self> {
        Box::new(Self { table_name, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let table = txn.must_get_table(&self.table_name)?;
        let rows = txn.scan_table(self.table_name.clone(), self.filter)?;

        Ok(ResultSet::Scan {
            columns: table.columns.iter().map(|c| c.name.clone()).collect(),
            rows,
        })
    }
}

pub struct Order<T> {
    order_by: Vec<(String, OrderDirection)>,
    source: Box<dyn Executor<T>>,
}

impl<T: Transaction> Order<T> {
    pub fn new(order_by: Vec<(String, OrderDirection)>, source: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { order_by, source })
    }
}
impl<T: Transaction> Executor<T> for Order<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, mut rows } => {
                let mut order_cor_index = HashMap::new();
                for (i, (col_name, _)) in self.order_by.iter().enumerate() {
                    match columns.iter().position(|c| *c == *col_name) {
                        Some(pos) => order_cor_index.insert(i, pos),
                        None => {
                            return Err(Error::InternalError(format!(
                                "order by colum {col_name} isn't in table"
                            )))
                        }
                    };
                }

                rows.sort_by(|col1, col2| {
                    for (i, (_, direction)) in self.order_by.iter().enumerate() {
                        let col_index = order_cor_index.get(&i).unwrap();
                        let col1 = &col1[*col_index];
                        let col2 = &col2[*col_index];
                        match col1.partial_cmp(col2) {
                            None => {}
                            Some(Ordering::Equal) => {}
                            Some(o) => {
                                return if *direction == OrderDirection::Asc {
                                    o
                                } else {
                                    o.reverse()
                                }
                            }
                        }
                    }
                    Ordering::Equal
                });

                Ok(ResultSet::Scan { columns, rows })
            }

            _ => return Err(Error::InternalError("Unexpected result set".into())),
        }
    }
}
