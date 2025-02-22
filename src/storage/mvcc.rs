use std::sync::{Arc, Mutex};

use crate::error::Result;

use super::engine::Engine;

pub struct Mvcc<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Mvcc {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        Ok(MvccTransaction::begin(self.engine.clone()))
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> MvccTransaction<E> {
    pub fn begin(eng: Arc<Mutex<E>>) -> Self {
        Self { engine: eng }
    }

    pub fn commit(self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut engine = self.engine.lock()?;
        engine.set(key, value)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;
        engine.get(key)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut engine = self.engine.lock()?;
        let mut iter = engine.scan_prefix(prefix);
        let mut res = Vec::new();
        while let Some((k, v)) = iter.next().transpose()? {
            res.push(ScanResult { key: k, value: v });
        }

        Ok(res)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
