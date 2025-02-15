use crate::error::Result;
use crate::storage;

use super::{Engine, Transaction};

pub struct KVEngine {
    pub kv: storage::Mvcc,
}

impl Clone for KVEngine {
    fn clone(&self) -> Self {
        KVEngine {
            kv: self.kv.clone(),
        }
    }
}

impl Engine for KVEngine {
    type Transaction = KVTransaction;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(KVTransaction::new(self.kv.begin()?))
    }
}

/// KV Transaction definition, actually a wrapper for MvccTransaction in the storage engine.
pub struct KVTransaction {
    txn: storage::MvccTransaction,
}

impl KVTransaction {
    pub fn new(txn: storage::MvccTransaction) -> KVTransaction {
        KVTransaction { txn }
    }
}

impl Transaction for KVTransaction {
    fn commit(&mut self) -> Result<()> {
        todo!()
    }

    fn rollback(&mut self) -> Result<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, raw: crate::sql::types::Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<crate::sql::types::Row>> {
        todo!()
    }

    fn crate_table(&mut self, table: crate::sql::schema::Table) -> Result<()> {
        todo!()
    }

    fn get_table(&self, table_name: String) -> Result<Option<crate::sql::schema::Table>> {
        todo!()
    }
}
