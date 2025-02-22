use crate::error::Result;
use crate::storage::{self, engine::EngineIterator};
use std::collections::BTreeMap;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;

const LOG_HEADER_SIZE: u32 = 8;

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;

pub struct BitCastDiskEngine {
    key_dir: KeyDir,
    log: Log,
}

impl storage::Engine for BitCastDiskEngine {
    type EngineIterator<'a> = BitcaskDiskEngineIterator;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // write to log first
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // update memory index
        //100--------------|----150
        //                130
        // value size = 20
        let value_offset = offset + size - value.len() as u64;
        self.key_dir.insert(key, (value_offset, value.len() as u32));

        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.key_dir.get(&key) {
            Some((offset, size)) => {
                let value = self.log.read_value(*offset, *size)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.log.write_entry(&key, None)?;
        self.key_dir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        todo!()
    }
}

pub struct BitcaskDiskEngineIterator {}

impl EngineIterator for BitcaskDiskEngineIterator {}

impl Iterator for BitcaskDiskEngineIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for BitcaskDiskEngineIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
struct Log {
    file: std::fs::File,
}

impl Log {
    /// +-------------+-------------+----------------+----------------+ \
    /// | key len(4)  | val len(4)  | key (variant)   | val (variant) | \
    /// +-------------+-------------+----------------+----------------+ \
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u64)> {
        // first move the file cursor to the end of the file
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let value_size = value.map_or(0, |v| v.len() as u32);
        let total_size = key_size + value_size + LOG_HEADER_SIZE;

        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        // write the key size, value size, key, and value
        writer.write_all(&key_size.to_le_bytes())?;
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_le_bytes())?;
        writer.write_all(key)?;
        if let Some(value) = value {
            writer.write_all(value)?;
        }

        writer.flush()?;

        Ok((offset, total_size as u64))
    }

    fn read_value(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;

        Ok(buf)
    }
}
