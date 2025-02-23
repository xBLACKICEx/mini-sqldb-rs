use crate::error::Result;
use crate::storage::{self, engine::EngineIterator};

use fs4::fs_std::FileExt;
use std::{
    collections::{btree_map, BTreeMap},
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::RangeBounds,
    path::PathBuf,
};

const LOG_HEADER_SIZE: u32 = 8;

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;

pub struct BitCastDiskEngine {
    key_dir: KeyDir,
    log: Log,
}

impl BitCastDiskEngine {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        let mut log = Log::new(file_path)?;
        // Recover key_dir from the log
        let key_dir = log.build_key_dir()?;

        Ok(Self { key_dir, log })
    }

    pub fn new_compact(file_path: PathBuf) -> Result<Self> {
        let mut eng = Self::new(file_path)?;
        eng.compact()?;

        Ok(eng)
    }

    fn compact(&mut self) -> Result<()> {
        // open a new tmp log file
        let mut new_path = self.log.file_path.clone();
        new_path.set_extension("compact");

        let mut new_log = Log::new(new_path)?;
        let new_key_dir = self
            .key_dir
            .iter()
            .map(|(key, (offset, val_size))| {
                // read the value from the old log
                let value = self.log.read_value(*offset, *val_size)?;
                let (new_offset, new_size) = new_log.write_entry(&key, Some(&value))?;
                let total_offset = new_offset + new_size as u64 - *val_size as u64;

                Ok((key.clone(), (total_offset, *val_size)))
            })
            .collect::<Result<KeyDir>>()?;

        // rename the new log file to the old one
        std::fs::rename(&new_log.file_path, &self.log.file_path)?;

        new_log.file_path = self.log.file_path.clone();
        self.key_dir = new_key_dir;
        self.log = new_log;

        Ok(())
    }
}

impl storage::Engine for BitCastDiskEngine {
    type EngineIterator<'a> = BitcaskDiskEngineIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // write to log first
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // update memory index
        //100--------------|----150
        //                130
        // value size = 20
        let value_offset = offset + size as u64 - value.len() as u64;
        self.key_dir.insert(key, (value_offset, value.len() as u32));

        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.key_dir.get(&key) {
            Some((val_offset, val_len)) => {
                let value = self.log.read_value(*val_offset, *val_len)?;
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
        BitcaskDiskEngineIterator {
            inner: self.key_dir.range(range),
            log: &mut self.log,
        }
    }
}

pub struct BitcaskDiskEngineIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl BitcaskDiskEngineIterator<'_> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (k, (offset, val_size)) = item;
        let value = self.log.read_value(*offset, *val_size)?;
        Ok((k.clone(), value))
    }
}

impl EngineIterator for BitcaskDiskEngineIterator<'_> {}

impl DoubleEndedIterator for BitcaskDiskEngineIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}

impl Iterator for BitcaskDiskEngineIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

struct Log {
    file_path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(file_path: PathBuf) -> Result<Self> {
        // if directory not exist create it
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)?;

        // add exclusive lock to the file, to be sure only one process can use it
        file.try_lock_exclusive()?;

        Ok(Self { file, file_path })
    }

    fn build_key_dir(&mut self) -> Result<KeyDir> {
        let mut key_dir = KeyDir::new();
        let mut buf_reader = BufReader::new(&self.file);
        let file_size = self.file.metadata()?.len();
        let mut offset = 0;

        while offset < file_size {
            let (key, val_len) = Self::read_entry(&mut buf_reader, offset)?;
            let val_offset = offset + LOG_HEADER_SIZE as u64 + key.len() as u64;

            match val_len {
                Some(val_len) => {
                    key_dir.insert(key, (val_offset, val_len));
                    offset = val_offset + val_len as u64;
                }
                None => {
                    key_dir.remove(&key);
                    offset = val_offset;
                }
            }
        }

        Ok(key_dir)
    }

    /// +-------------+-------------+----------------+----------------+ \
    /// | key len(4)  | val len(4)  | key (variant)   | val (variant) | \
    /// +-------------+-------------+----------------+----------------+ \
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // first move the file cursor to the end of the file
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let value_size = value.map_or(u32::MAX, |v| v.len() as u32);

        let payload_size = if value_size == u32::MAX {
            0
        } else {
            value_size
        };
        let total_size = key_size + payload_size + LOG_HEADER_SIZE;

        // write the key size, value size, key, and value
        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        writer.write_all(&key_size.to_le_bytes())?;
        writer.write_all(&value_size.to_le_bytes())?;
        writer.write_all(key)?;
        if let Some(v) = value {
            writer.write_all(v)?;
        }
        writer.flush()?;

        Ok((offset, total_size))
    }

    fn read_value(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;

        Ok(buf)
    }

    fn read_entry(
        buf_reader: &mut BufReader<&File>,
        offset: u64,
    ) -> Result<(Vec<u8>, Option<u32>)> {
        buf_reader.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4];

        // read key size
        buf_reader.read_exact(&mut len_buf)?;
        let key_size = u32::from_le_bytes(len_buf);
        // read value size
        buf_reader.read_exact(&mut len_buf)?;
        let val_size = u32::from_le_bytes(len_buf);

        // read key
        let mut key_buf = vec![0; key_size as usize];
        buf_reader.read_exact(&mut key_buf)?;
        // read value
        let value_buf = match val_size {
            u32::MAX => None,
            _ => {
                let mut value_buf = vec![0; val_size as usize];
                buf_reader.read_exact(&mut value_buf)?;
                Some(value_buf)
            }
        };

        if val_size != u32::MAX {
            buf_reader.seek(SeekFrom::Current(val_size as i64))?;
            Ok((key_buf, Some(val_size)))
        } else {
            Ok((key_buf, None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BitCastDiskEngine;
    use crate::{error::Result, storage::Engine};
    use std::env;

    #[test]
    fn test_disk_engine_compact() -> Result<()> {
        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb-bitcast/test_bitcast_disk_compact.mrdb.log");
        let mut eng = BitCastDiskEngine::new(temp_file.clone())?;

        // write some data
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;

        // rewrite
        eng.set(b"aa".to_vec(), b"value1".to_vec())?;
        eng.set(b"aa".to_vec(), b"value2".to_vec())?;
        eng.set(b"aa".to_vec(), b"value3".to_vec())?;
        eng.set(b"bb".to_vec(), b"value4".to_vec())?;
        eng.set(b"bb".to_vec(), b"value5".to_vec())?;

        let iter = eng.scan(..);
        let v = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng);

        // compact the log
        let mut eng2 = BitCastDiskEngine::new_compact(temp_file.clone())?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng2);

        std::fs::remove_dir_all(temp_file.parent().unwrap())?;

        Ok(())
    }
}
