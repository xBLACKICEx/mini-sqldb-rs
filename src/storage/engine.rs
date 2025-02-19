use std::ops::{Bound, RangeBounds};

use crate::error::Result;

pub trait Engine {
    type EenginIterator<'a>: EngineIterator
    where
        Self: 'a;

    // set a key-value pair
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    // get the value of a key
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    // delete a key coreesponding value if not exist ignore
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EenginIterator<'_>;

    fn scan_prefix(&mut self, mut prefix: Vec<u8>) -> Self::EenginIterator<'_> {
        // start: aaaa
        // end: aaab
        let start = Bound::Included(prefix.clone());

        if let Some(last) = prefix.last_mut() {
            // Use wrapping_add to avoid overflow risk
            *last += last.wrapping_add(1);
        }
        let end = Bound::Excluded(prefix);

        self.scan((start, end))
    }
}

pub trait EngineIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

#[cfg(test)]
mod tests {
    use super::Engine;
    use crate::{error::Result, storage::memory::MemoryEngine};
    use std::ops::Bound;

    #[test]
    fn test_memory() -> Result<()> {
        test_point_operations(MemoryEngine::new())?;
        test_scan_operations(MemoryEngine::new())?;
        test_scan_prefix_operations(MemoryEngine::new())?;
        test_scan_prefix_overflow(MemoryEngine::new())?;
        Ok(())
    }

    fn test_point_operations(mut eng: impl Engine) -> Result<()> {
        println!("Testing point operations...");

        // Test get a non-existent key
        println!("- Testing get non-existent key");
        assert_eq!(eng.get(b"not exist".to_vec())?, None);

        // Test set and get an existing key
        println!("- Testing set and get existing key");
        eng.set(b"aa".to_vec(), vec![1, 2, 3, 4])?;
        assert_eq!(eng.get(b"aa".to_vec())?, Some(vec![1, 2, 3, 4]));

        // Test repeat put (overwrite)
        println!("- Testing repeat put (overwrite)");
        eng.set(b"aa".to_vec(), vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"aa".to_vec())?, Some(vec![5, 6, 7, 8]));

        // Test delete and then read
        println!("- Testing delete and then read");
        eng.delete(b"aa".to_vec())?;
        assert_eq!(eng.get(b"aa".to_vec())?, None);

        // Test empty key and value
        println!("- Testing empty key and value");
        assert_eq!(eng.get(b"".to_vec())?, None);
        eng.set(b"".to_vec(), vec![])?;
        assert_eq!(eng.get(b"".to_vec())?, Some(vec![]));

        // Test another key-value pair
        println!("- Testing another key-value pair");
        eng.set(b"cc".to_vec(), vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"cc".to_vec())?, Some(vec![5, 6, 7, 8]));

        println!("Point operations test passed!\n");
        Ok(())
    }

    fn test_scan_operations(mut eng: impl Engine) -> Result<()> {
        println!("Testing scan operations...");

        eng.set(b"nnaes".to_vec(), b"value1".to_vec())?;
        eng.set(b"amhue".to_vec(), b"value2".to_vec())?;
        eng.set(b"meeae".to_vec(), b"value3".to_vec())?;
        eng.set(b"uujeh".to_vec(), b"value4".to_vec())?;
        eng.set(b"anehe".to_vec(), b"value5".to_vec())?;

        // Test forward scan
        println!("- Testing forward scan");
        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"e".to_vec());

        let mut iter = eng.scan((start.clone(), end.clone()));
        let (key1, _) = iter.next().expect("no value founded")?;
        assert_eq!(key1, b"amhue".to_vec());

        let (key2, _) = iter.next().expect("no value founded")?;
        assert_eq!(key2, b"anehe".to_vec());
        drop(iter);

        // Test backward scan
        println!("- Testing backward scan");
        let start = Bound::Included(b"b".to_vec());
        let end = Bound::Excluded(b"z".to_vec());

        let mut iter2 = eng.scan((start, end));

        let (key3, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key3, b"uujeh".to_vec());

        let (key4, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key4, b"nnaes".to_vec());

        let (key5, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key5, b"meeae".to_vec());

        println!("Scan operations test passed!\n");
        Ok(())
    }

    fn test_scan_prefix_operations(mut eng: impl Engine) -> Result<()> {
        println!("Testing scan prefix operations...");

        eng.set(b"ccnaes".to_vec(), b"value1".to_vec())?;
        eng.set(b"camhue".to_vec(), b"value2".to_vec())?;
        eng.set(b"deeae".to_vec(), b"value3".to_vec())?;
        eng.set(b"eeujeh".to_vec(), b"value4".to_vec())?;
        eng.set(b"canehe".to_vec(), b"value5".to_vec())?;
        eng.set(b"aanehe".to_vec(), b"value6".to_vec())?;

        // Test scan with prefix "ca"
        println!("- Testing scan with prefix 'ca'");
        let prefix = b"ca".to_vec();
        let mut iter = eng.scan_prefix(prefix);
        let (key1, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key1, b"camhue".to_vec());
        let (key2, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key2, b"canehe".to_vec());

        println!("Scan prefix operations test passed!\n");
        Ok(())
    }

    fn test_scan_prefix_overflow(mut eng: impl Engine) -> Result<()> {
        println!("Testing overflow case...");

        // Construct a key with the last byte as 255
        let key = vec![b'a', b'b', 255];
        eng.set(key.clone(), b"value_overflow".to_vec())?;

        // Call scan_prefix, at this time the last byte of prefix will become 0 by wrapping add 1 from 255, the formed range does not have any key
        let mut iter = eng.scan_prefix(key);
        assert!(iter.next().is_none());

        println!("Overflow case test passed!\n");
        Ok(())
    }
}
