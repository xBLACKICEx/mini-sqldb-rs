use serde::{Deserialize, Serialize};

use super::Transaction;
use crate::error::{Error, Result};
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::storage::mvcc;
use crate::{sql, storage};

pub struct KVEngine<E: storage::Engine> {
    pub kv: storage::Mvcc<E>,
}

impl<E: storage::Engine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::Mvcc::new(engine),
        }
    }
}

impl<E: storage::Engine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        KVEngine {
            kv: self.kv.clone(),
        }
    }
}

impl<E: storage::Engine> sql::Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(KVTransaction::new(self.kv.begin()?))
    }
}

/// KV Transaction definition, actually a wrapper for MvccTransaction in the storage engine.
pub struct KVTransaction<E: storage::Engine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: storage::Engine> KVTransaction<E> {
    pub fn new(txn: mvcc::MvccTransaction<E>) -> KVTransaction<E> {
        KVTransaction { txn }
    }
}

impl<E: storage::Engine> Transaction for KVTransaction<E> {
    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }

    fn crate_table(&mut self, table: Table) -> Result<()> {
        // check if table exists
        if self.get_table(&table.name)?.is_some() {
            return Err(Error::InternalError(format!(
                "Table {} already exists",
                table.name
            )));
        }

        // Validate the table
        if table.columns.is_empty() {
            return Err(Error::InternalError(format!(
                "Table {} has no columns",
                table.name
            )));
        }

        // create table
        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;
        self.txn.set(bincode::serialize(&key)?, value)?;

        Ok(())
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(&table_name)?;
        // Validate the row
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None => {
                    if !col.nullable {
                        return Err(Error::InternalError(format!(
                            "Column {} expects type {:?}, got NULL",
                            col.name, col.datatype
                        )));
                    }
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::InternalError(format!(
                        "Column {} expects type {:?}, got {:?}",
                        col.name, col.datatype, dt
                    )))
                }
                _ => {}
            }
        }

        // Store data
        // TODO Temporarily use the first column as the primary key, a unique identifier for a row of data.
        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;

        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;

        let mut rows = vec![];
        for result in results {
            let row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }

        Ok(rows)
    }

    fn get_table(&self, table_name: &str) -> Result<Option<Table>> {
        let key = Key::Table(table_name.to_string());
        let v = self
            .txn
            .get(bincode::serialize(&key)?)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?;

        Ok(v)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    /// For table metadata
    Table(String),
    /// For table rows: (table_name, primary_key_value)
    Row(String, Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::sql::schema::{Column, Table};
    use crate::sql::types::{DataType, Row, Value};
    use crate::sql::Engine;
    use crate::storage::memory::MemoryEngine;

    // Test helper functions module
    mod helpers {
        use super::*;
        use crate::sql::executor::ResultSet;

        pub fn create_test_table(name: &str) -> Table {
            Table {
                name: name.to_string(),
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        datatype: DataType::Integer,
                        nullable: false,
                        default: None,
                    },
                    Column {
                        name: "name".to_string(),
                        datatype: DataType::String,
                        nullable: true,
                        default: Some(Value::Null),
                    },
                    Column {
                        name: "age".to_string(),
                        datatype: DataType::Integer,
                        nullable: true,
                        default: Some(Value::Null),
                    },
                ],
            }
        }

        pub fn create_test_rows() -> Vec<Row> {
            vec![
                vec![
                    Value::Integer(1),
                    Value::String("Alice".to_string()),
                    Value::Integer(30),
                ],
                vec![
                    Value::Integer(2),
                    Value::String("Bob".to_string()),
                    Value::Integer(25),
                ],
            ]
        }

        pub fn run_table_tests<E: storage::Engine>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let mut txn = kv_engine.begin()?;

            let table = create_test_table("test_table");
            txn.crate_table(table.clone())?;

            let stored_table = txn.get_table(&table.name)?;
            assert_eq!(stored_table, Some(table.clone()));

            // Test duplicate table creation
            assert!(txn.crate_table(table.clone()).is_err());

            // Test creating a table with empty columns
            let empty_table = Table {
                name: "empty".to_string(),
                columns: vec![],
            };
            assert!(txn.crate_table(empty_table).is_err());

            txn.commit()?;
            Ok(())
        }

        pub fn run_row_tests<E: storage::Engine>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let mut txn = kv_engine.begin()?;

            // Create table
            let table = create_test_table("test_table");
            txn.crate_table(table.clone())?;

            // Insert test data
            let test_rows = create_test_rows();
            for row in test_rows.iter() {
                txn.create_row(table.name.clone(), row.clone())?;
            }

            // Verify data
            let stored_rows = txn.scan_table(table.name.clone())?;
            assert_eq!(stored_rows.len(), test_rows.len());
            for (stored, expected) in stored_rows.iter().zip(test_rows.iter()) {
                assert_eq!(stored, expected);
            }

            txn.commit()?;
            Ok(())
        }

        pub fn run_invalid_row_tests<E: storage::Engine>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let mut txn = kv_engine.begin()?;

            let table = create_test_table("test_table");
            txn.crate_table(table.clone())?;

            // Test inserting data with mismatched types
            let invalid_type_row = vec![
                Value::String("invalid".to_string()), // Should be an integer
                Value::String("name".to_string()),
                Value::Integer(30),
            ];
            assert!(txn
                .create_row(table.name.clone(), invalid_type_row)
                .is_err());

            // Test inserting NULL into a non-nullable column
            let null_in_notnull = vec![
                Value::Null, // id column cannot be NULL
                Value::String("name".to_string()),
                Value::Integer(30),
            ];
            assert!(txn.create_row(table.name.clone(), null_in_notnull).is_err());

            txn.commit()?;
            Ok(())
        }

        pub fn run_sql_tests<E: storage::Engine>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let session = kv_engine.session()?;

            // Use SQL to create a table
            let result = session
                .execute("CREATE TABLE test_table (id INT NOT NULL, name TEXT, age INT);")?;
            match result {
                ResultSet::CrateTable { table_name } => {
                    assert_eq!(table_name, "test_table");
                }
                _ => panic!("Expected CrateTable result"),
            }

            // Verify table creation
            let mut txn = session.engine.begin()?;
            let table = create_test_table("test_table");
            assert_eq!(txn.get_table(&table.name)?, Some(table));
            txn.commit()?;

            // Insert data
            let result = session
                .execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Alice', 30);")?;
            match result {
                ResultSet::Insert { count } => {
                    assert_eq!(count, 1);
                }
                _ => panic!("Expected Insert result"),
            }

            let result =
                session.execute("INSERT INTO test_table (id, name, age) VALUES (2, 'Bob', 25);")?;
            match result {
                ResultSet::Insert { count } => {
                    assert_eq!(count, 1);
                }
                _ => panic!("Expected Insert result"),
            }

            // Query data
            let result = session.execute("SELECT * FROM test_table;")?;
            if let ResultSet::Scan { columns: _, rows } = result {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows, create_test_rows());
            } else {
                panic!("Expected Scan result");
            }

            Ok(())
        }

        pub fn run_sql_error_tests<E: storage::Engine>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let session = kv_engine.session()?;

            // Create table
            session.execute("CREATE TABLE test_table (id INT NOT NULL, name TEXT, age INT);")?;

            // Duplicate table creation should fail
            assert!(session
                .execute("CREATE TABLE test_table (id INT NOT NULL, name TEXT, age INT);")
                .is_err());

            // Inserting data with mismatched types should fail
            assert!(session
                .execute(
                    "INSERT INTO test_table (id, name, age) VALUES ('invalid', 'Charlie', 35);"
                )
                .is_err());

            // Inserting data into a non-existent table should fail
            assert!(session
                .execute("INSERT INTO nonexistent (id) VALUES (1);")
                .is_err());

            Ok(())
        }
    }

    // Arrange tests in order
    #[test]
    fn test_memory_engine_table_operations() -> Result<()> {
        helpers::run_table_tests(MemoryEngine::new())
    }

    #[test]
    fn test_memory_engine_row_operations() -> Result<()> {
        helpers::run_row_tests(MemoryEngine::new())
    }

    #[test]
    fn test_memory_engine_invalid_row_operations() -> Result<()> {
        helpers::run_invalid_row_tests(MemoryEngine::new())
    }

    #[test]
    fn test_memory_engine_sql_operations() -> Result<()> {
        helpers::run_sql_tests(MemoryEngine::new())
    }

    #[test]
    fn test_memory_engine_sql_error_cases() -> Result<()> {
        helpers::run_sql_error_tests(MemoryEngine::new())
    }
}
