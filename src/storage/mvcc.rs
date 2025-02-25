use super::engine::Engine;
use crate::error::{Error, Result};

use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard},
};

pub type Version = u64;

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
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone())
    }
}

/// Internal metadata key types for MVCC
#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    /// Stores the next available version number (persistent counter)
    /// - Purpose: Ensure the uniqueness and increment of transaction version numbers.
    NextVersion,

    /// Records the version numbers of active transactions (uncommitted transactions, \
    /// deleted after transaction commit), used for conflict detection and visibility judgment.
    TxnActive(Version),

    /// Records the write operations of the transaction (used for rollback)
    /// - Key format: {version} - {key}
    /// - Purpose: Record which transaction keys were modified by the transaction, used to clean up
    /// corresponding versions during transaction rollback.
    TxnWrite(Version, Vec<u8>),

    /// Actually stored transaction version
    /// - Key format: {key} - {version}
    /// - Purpose: Store the value of the transaction key under a specific version, achieving multi-version coexistence.
    Version(Vec<u8>, Version),
}

impl MvccKey {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }   

    pub fn decode(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

#[derive(Debug, Serialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

pub struct TransactionState {
    /// current Transaction version
    pub version: Version,
    /// current Active Transaction Version List
    pub active_versions: HashSet<Version>,
}

impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        // Rules:
        // 1. If version belongs to active transaction -> Not visible
        // 2. Version number <= current transaction version -> Visible
        !self.active_versions.contains(&version) && version <= self.version
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

impl<E: Engine> MvccTransaction<E> {
    /// Start a transaction (get version number, record active transactions)
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        let mut engine = eng.lock()?;
        // get newest version
        let next_version = match engine.get(MvccKey::NextVersion.encode())? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1, // initial version number
        };
        // increment next version
        engine.set(
            MvccKey::NextVersion.encode(),
            bincode::serialize(&(next_version + 1))?,
        )?;

        // get current active transactions
        let active_versions = Self::scan_active(&mut engine)?;

        // mark current transaction as active
        engine.set(MvccKey::TxnActive(next_version).encode(), vec![])?;

        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            },
        })
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut eng = self.engine.lock()?;
        eng.get(key)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult { key, value });
        }
        Ok(results)
    }

    /// Internal write handler (conflict detection)
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // Get the storage engine
        let mut engine = self.engine.lock()?;

        // Detect conflicts
        //  3 4 5
        //  6
        //  key1-3 key2-4 key3-5
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1), // Exclude current transactions
        )
        .encode();
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();
        //  Current active transaction list 3 4 5
        //  Current transaction 6
        // Only need to check the last version number
        // 1. Keys are sorted in order, and the scanned results are from small to large
        // 2. If a new transaction modifies this key, such as 10, and 10 commits after modification, then 6 modifying this key will be a conflict
        // 3. If the current active transaction modifies this key, such as 4, then transaction 5 cannot modify this key
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(&k)? {
                MvccKey::Version(_, version) => {
                    // Check if this version is visible
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::InternalError(format!(
                        "unexpected Mvcc key: {:?}",
                        String::from_utf8(k)
                    )))
                }
            }
        }

        // Record which keys this version wrote, for transaction rollback
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode(),
            vec![],
        )?;

        // Write the actual key-value data
        engine.set(
            MvccKey::Version(key, self.state.version).encode(),
            bincode::serialize(&value)?,
        )
    }

    // Scan to get all active transactions listed in the engine
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode());

        while let Some((key, _)) = iter.next().transpose()? {
            if let MvccKey::TxnActive(v) = MvccKey::decode(&key)? {
                active_versions.insert(v);
            } else {
                return Err(Error::InternalError(format!(
                    "unexpected Mvcc key: {:?}",
                    String::from_utf8(key)
                )));
            }
        }

        Ok(active_versions)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
