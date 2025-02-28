use std::ops::{Bound, RangeBounds};

use crate::error::Result;

pub trait Engine {
    type EngineIterator<'a>: EngineIterator
    where
        Self: 'a;

    // set a key-value pair
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    // get the value of a key
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    // delete a key corresponding value if not exist ignore
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_>;

    // Scans for all key-value pairs where the key starts with the given prefix
    fn scan_prefix(&mut self, prefix: Vec<u8>) -> Self::EngineIterator<'_> {
        // Special case: empty prefix should return all key-value pairs
        if prefix.is_empty() {
            return self.scan(..); // Full range scan
        }

        // Define the start bound: keys must be >= prefix
        let start = Bound::Included(prefix.clone());

        // Calculate the end bound: the first key that would not start with the prefix
        let end = {
            let mut bound_prefix = prefix;

            // To find the end bound, we need to find the lexicographically smallest key
            // that doesn't start with the prefix. This is done by incrementing the last
            // non-0xFF byte and truncating.

            // Find the first non-0xFF byte from right to left
            let mut i = bound_prefix.len();
            while i > 0 {
                i -= 1;
                if bound_prefix[i] < 0xFF {
                    // If we find a byte that isn't 0xFF, increment it and truncate
                    // Example: prefix "ab\x01" becomes "ab\x02" (everything after is truncated)
                    bound_prefix[i] += 1;
                    bound_prefix.truncate(i + 1);
                    break;
                } else if i == 0 {
                    // Edge case: All bytes are 0xFF (e.g., "\xFF\xFF\xFF")
                    // In this case, there's no clear "next" prefix, so we use Unbounded
                    // This means we'll scan from the prefix to the end of the database
                    return self.scan((start, Bound::Unbounded));
                }
            }

            // We exclude the end bound since we want keys strictly less than this value
            // Example: scan_prefix("ab") will scan keys from "ab" (inclusive) to "ac" (exclusive)
            Bound::Excluded(bound_prefix)
        };

        // Perform a range scan with our calculated bounds
        self.scan((start, end))
    }
}

pub trait EngineIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

#[cfg(test)]
mod tests {
    use super::Engine;
    use crate::error::Result;
    use std::ops::Bound;

    #[test]
    fn test_memory() -> Result<()> {
        use crate::storage::memory::MemoryEngine;

        test_point_operations(MemoryEngine::new())?;
        test_scan_operations(MemoryEngine::new())?;

        test_scan_prefix_operations(MemoryEngine::new())?;
        test_scan_prefix_basic(MemoryEngine::new())?;
        test_scan_prefix_single_byte_overflow(MemoryEngine::new())?;
        test_scan_prefix_multi_byte_overflow(MemoryEngine::new())?;
        test_scan_prefix_empty(MemoryEngine::new())?;
        test_scan_prefix_mixed_overflow(MemoryEngine::new())?;
        Ok(())
    }

    #[test]
    fn test_bitcast_disk() -> Result<()> {
        use crate::storage::bitcast_disk::BitCastDiskEngine;
        use std::env;

        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb/test_bitcast_disk.mrdb.log");
        test_point_operations(BitCastDiskEngine::new(temp_file.clone())?)?;

        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb/test_bitcast_disk2.mrdb.log");
        test_scan_operations(BitCastDiskEngine::new(temp_file.clone())?)?;

        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb/test_bitcast_disk4.mrdb.log");
        test_scan_prefix_basic(BitCastDiskEngine::new(temp_file.clone())?)?;

        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb/test_bitcast_disk5.mrdb.log");
        test_scan_prefix_single_byte_overflow(BitCastDiskEngine::new(temp_file.clone())?)?;

        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb/test_bitcast_disk6.mrdb.log");
        test_scan_prefix_multi_byte_overflow(BitCastDiskEngine::new(temp_file.clone())?)?;

        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb/test_bitcast_disk7.mrdb.log");
        test_scan_prefix_empty(BitCastDiskEngine::new(temp_file.clone())?)?;

        let mut temp_file = env::temp_dir();
        temp_file.push("sqldb/test_bitcast_disk8.mrdb.log");
        test_scan_prefix_mixed_overflow(BitCastDiskEngine::new(temp_file.clone())?)?;

        std::fs::remove_dir_all(temp_file.parent().unwrap())?;
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

    fn test_scan_prefix_basic(mut eng: impl Engine) -> Result<()> {
        println!("Testing basic prefix...");

        // Insert data
        eng.set(vec![b'a', b'b', 1], b"val1".to_vec())?;
        eng.set(vec![b'a', b'b', 2], b"val2".to_vec())?;
        eng.set(vec![b'a', b'c', 1], b"val3".to_vec())?; // Should not be scanned

        // Scan prefix ab
        let iter = eng.scan_prefix(vec![b'a', b'b']);
        let mut count = 0;
        for result in iter {
            let (key, _) = result?;
            assert!(
                key.starts_with(&[b'a', b'b']),
                "Key {:?} does not belong to prefix ab",
                key
            );
            count += 1;
        }
        assert_eq!(count, 2, "Should find 2 keys, but found {}", count);

        println!("Basic test passed!\n");
        Ok(())
    }

    fn test_scan_prefix_single_byte_overflow(mut eng: impl Engine) -> Result<()> {
        println!("Testing single byte overflow...");

        // Insert keys starting with ab\xff
        eng.set(vec![b'a', b'b', 0xff], b"v1".to_vec())?;
        eng.set(vec![b'a', b'b', 0xff, 0x01], b"v2".to_vec())?;
        eng.set(vec![b'a', b'c', 0x00], b"v3".to_vec())?; // Should not be scanned

        // Scan prefix ab\xff
        let iter = eng.scan_prefix(vec![b'a', b'b', 0xff]);
        let expected_keys = [vec![b'a', b'b', 0xff], vec![b'a', b'b', 0xff, 0x01]];
        let mut found_keys = Vec::new();
        for result in iter {
            let (key, _) = result?;
            found_keys.push(key);
        }
        assert_eq!(
            found_keys, expected_keys,
            "Did not find the expected overflow keys"
        );

        println!("Single byte overflow test passed!\n");
        Ok(())
    }

    fn test_scan_prefix_multi_byte_overflow(mut eng: impl Engine) -> Result<()> {
        println!("Testing multi-byte continuous overflow...");

        // Insert keys 0xff\xff and 0xff\xff\xff
        eng.set(vec![0xff, 0xff], b"v1".to_vec())?;
        eng.set(vec![0xff, 0xff, 0xff], b"v2".to_vec())?;
        eng.set(vec![0x00, 0x00], b"v3".to_vec())?; // Should not be scanned

        // Scan prefix 0xff\xff
        let iter = eng.scan_prefix(vec![0xff, 0xff]);
        let expected_keys = [vec![0xff, 0xff], vec![0xff, 0xff, 0xff]];
        let mut found_keys = Vec::new();
        for result in iter {
            let (key, _) = result?;
            found_keys.push(key);
        }
        assert_eq!(
            found_keys, expected_keys,
            "Carry was not correctly propagated"
        );

        println!("Multi-byte overflow test passed!\n");
        Ok(())
    }

    fn test_scan_prefix_empty(mut eng: impl Engine) -> Result<()> {
        println!("Testing empty prefix...");

        // Insert keys with different prefixes
        eng.set(vec![b'a'], b"v1".to_vec())?;
        eng.set(vec![b'b', 0x01], b"v2".to_vec())?;
        eng.set(vec![0x00], b"v3".to_vec())?;

        // Scan empty prefix (should return all keys)
        let iter = eng.scan_prefix(vec![]);
        let mut count = 0;
        for result in iter {
            result?; // Just check for errors
            count += 1;
        }
        assert_eq!(count, 3, "Should find 3 keys, but found {}", count);

        println!("Empty prefix test passed!\n");
        Ok(())
    }

    fn test_scan_prefix_mixed_overflow(mut eng: impl Engine) -> Result<()> {
        println!("Testing mixed carry...");

        // Insert keys a\xff\xff and a\xff\xff\xff
        eng.set(vec![b'a', 0xff, 0xff], b"v1".to_vec())?;
        eng.set(vec![b'a', 0xff, 0xff, 0xff], b"v2".to_vec())?;
        eng.set(vec![b'b', 0x00, 0x00], b"v3".to_vec())?; // Should not be scanned

        // Scan prefix a\xff\xff
        let iter = eng.scan_prefix(vec![b'a', 0xff, 0xff]);
        let expected_keys = [vec![b'a', 0xff, 0xff], vec![b'a', 0xff, 0xff, 0xff]];
        let mut found_keys = Vec::new();
        for result in iter {
            let (key, _) = result?;
            found_keys.push(key);
        }
        assert_eq!(found_keys, expected_keys, "Mixed carry error");

        println!("Mixed carry test passed!\n");
        Ok(())
    }
}
