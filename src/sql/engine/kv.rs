use serde::{Deserialize, Serialize};

use super::Transaction;
use crate::error::{Error, Result};
use crate::sql::parser::ast::Expression;
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::storage::keycode::serialize_key;
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
        self.txn.commit()
    }

    fn rollback(&mut self) -> Result<()> {
        self.txn.rollback()
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // check if table exists
        if self.get_table(&table.name)?.is_some() {
            return Err(Error::InternalError(format!(
                "Table {} already exists",
                table.name
            )));
        }

        table.is_validate()?;

        // create table
        let key = Key::Table(table.name.clone()).encode()?;
        let value = bincode::serialize(&table)?;
        self.txn.set(key, value)?;

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
        let primary_key = table.get_primary_key(&row)?;
        let key = Key::Row(table_name.clone(), primary_key.clone()).encode()?;

        if self.txn.get(key.clone())?.is_some() {
            return Err(Error::InternalError(format!(
                "Duplicated data for primary key {} already exists in table {}",
                primary_key, table_name
            )));
        }

        let value = bincode::serialize(&row)?;
        //    K        V
        //  TN:PK      Row
        self.txn.set(key, value)?;
        Ok(())
    }

    fn scan_table(
        &mut self,
        table_name: String,
        filter: Option<(String, Expression)>,
    ) -> Result<Vec<Row>> {
        // TODO: Should be optimized.
        let prefix = KeyPrefix::Row(table_name.clone()).encode()?;
        let table = self.must_get_table(&table_name)?;
        let results = self.txn.scan_prefix(prefix)?;

        let mut rows = vec![];
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            if let Some((col, expr)) = &filter {
                let col_index = table.get_col_index(&col)?;
                if Value::from(expr) == row[col_index] {
                    rows.push(row);
                }
            } else {
                rows.push(row);
            }
        }

        Ok(rows)
    }

    fn get_table(&mut self, table_name: &str) -> Result<Option<Table>> {
        let key = Key::Table(table_name.to_string()).encode()?;
        let v = self
            .txn
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?;

        Ok(v)
    }

    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;

        if id != new_pk {
            let key = Key::Row(table.name.clone(), id.clone()).encode()?;
            self.txn.delete(key)?;
        }

        let key = Key::Row(table.name.clone(), new_pk.clone()).encode()?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    /// For table metadata
    Table(String),
    /// For table rows: (table_name, primary_key_value)
    Row(String, Value),
}

impl Key {
    fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

impl KeyPrefix {
    fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::sql::schema::{Column, Table};
    use crate::sql::types::{DataType, Row, Value};
    use crate::sql::Engine;
    use crate::storage::{bitcast_disk::BitCastDiskEngine, memory::MemoryEngine};

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

    #[test]
    fn test_memory_engine_primary_key_constraints() -> Result<()> {
        helpers::run_primary_key_tests(MemoryEngine::new())
    }

    #[test]
    fn test_bitcast_disk_engine_table_operations() -> Result<()> {
        let mut temp_file = std::env::temp_dir();
        temp_file.push("sqldb-bitcast/test_bitcast_disk_table.mrdb.log");
        helpers::run_table_tests(BitCastDiskEngine::new(temp_file.clone())?)?;
        std::fs::remove_file(temp_file)?;
        Ok(())
    }

    #[test]
    fn test_bitcast_disk_engine_row_operations() -> Result<()> {
        let mut temp_file = std::env::temp_dir();
        temp_file.push("sqldb-bitcast/test_bitcast_disk_row.mrdb.log");
        helpers::run_row_tests(BitCastDiskEngine::new(temp_file.clone())?)?;
        std::fs::remove_file(temp_file)?;
        Ok(())
    }

    #[test]
    fn test_bitcast_disk_engine_invalid_row_operations() -> Result<()> {
        let mut temp_file = std::env::temp_dir();
        temp_file.push("sqldb-bitcast/test_bitcast_disk_invalid_row.mrdb.log");
        helpers::run_invalid_row_tests(BitCastDiskEngine::new(temp_file.clone())?)?;
        std::fs::remove_file(temp_file)?;
        Ok(())
    }

    #[test]
    fn test_bitcast_disk_engine_sql_operations() -> Result<()> {
        let mut temp_file = std::env::temp_dir();
        temp_file.push("sqldb-bitcast/test_bitcast_disk_sql.mrdb.log");
        helpers::run_sql_tests(BitCastDiskEngine::new(temp_file.clone())?)?;
        std::fs::remove_file(temp_file)?;
        Ok(())
    }

    #[test]
    fn test_bitcast_disk_engine_sql_error_cases() -> Result<()> {
        let mut temp_file = std::env::temp_dir();
        temp_file.push("sqldb-bitcast/test_bitcast_disk_sql_error.mrdb.log");
        helpers::run_sql_error_tests(BitCastDiskEngine::new(temp_file.clone())?)?;
        std::fs::remove_file(temp_file)?;
        Ok(())
    }

    #[test]
    fn test_bitcast_disk_engine_primary_key_constraints() -> Result<()> {
        let mut temp_file = std::env::temp_dir();
        temp_file.push("sqldb-bitcast/test_bitcast_disk_primary_key.mrdb.log");
        helpers::run_primary_key_tests(BitCastDiskEngine::new(temp_file.clone())?)?;
        std::fs::remove_file(temp_file)?;
        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b', 2);")?;
        s.execute("insert into t1 values(3, 'c', 3);")?;

        let v = s.execute("update t1 set a = 33 where a = 3;")?;
        println!("{:?}", v);

        match s.execute("select * from t1;")? {
            crate::sql::executor::ResultSet::Scan { columns: _, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

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
                        primary_key: true,
                    },
                    Column {
                        name: "name".to_string(),
                        datatype: DataType::String,
                        nullable: true,
                        default: Some(Value::Null),
                        primary_key: false,
                    },
                    Column {
                        name: "age".to_string(),
                        datatype: DataType::Integer,
                        nullable: true,
                        default: Some(Value::Null),
                        primary_key: false,
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
            txn.create_table(table.clone())?;

            let stored_table = txn.get_table(&table.name)?;
            assert_eq!(stored_table, Some(table.clone()));

            // Test duplicate table creation
            assert!(txn.create_table(table.clone()).is_err());

            // Test creating a table with empty columns
            let empty_table = Table {
                name: "empty".to_string(),
                columns: vec![],
            };
            assert!(txn.create_table(empty_table).is_err());

            txn.commit()?;
            Ok(())
        }

        pub fn run_row_tests<E: storage::Engine>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let mut txn = kv_engine.begin()?;

            // Create table
            let table = create_test_table("test_table");
            txn.create_table(table.clone())?;

            // Insert test data
            let test_rows = create_test_rows();
            for row in test_rows.iter() {
                txn.create_row(table.name.clone(), row.clone())?;
            }

            txn.commit()?;
            // Verify data
            let stored_rows = txn.scan_table(table.name.clone(), None)?;
            println!("Stored rows: {:?}", stored_rows);
            println!("Expected rows: {:?}", test_rows);
            assert_eq!(stored_rows.len(), test_rows.len());
            for (stored, expected) in stored_rows.iter().zip(test_rows.iter()) {
                assert_eq!(stored, expected);
            }

            Ok(())
        }

        pub fn run_invalid_row_tests<E: storage::Engine>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let mut txn = kv_engine.begin()?;

            let table = create_test_table("test_table");
            txn.create_table(table.clone())?;

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
            txn.commit()?;

            assert!(txn.create_row(table.name.clone(), null_in_notnull).is_err());

            Ok(())
        }

        pub fn run_sql_tests<E: storage::Engine + 'static>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let session = kv_engine.session()?;

            // Use SQL to create a table
            let result = session.execute(
                "CREATE TABLE test_table (id INT PRIMARY KEY NOT NULL, name TEXT, age INT);",
            )?;
            match result {
                ResultSet::CreateTable { table_name } => {
                    assert_eq!(table_name, "test_table");
                }
                _ => panic!("Expected CrateTable result"),
            }

            // Verify table creation
            let mut txn = session.engine.begin()?;
            let table = create_test_table("test_table");
            txn.commit()?;
            assert_eq!(txn.get_table(&table.name)?, Some(table));

            // Insert data
            let result = session
                .execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Alice', 30);")?;
            println!("Result: {:?}", result);
            match result {
                ResultSet::Insert { count } => {
                    assert_eq!(count, 1);
                }
                _ => panic!("Expected Insert result"),
            }

            // Insert data without column
            let result = session.execute("INSERT INTO test_table VALUES (2, 'Bob', 25);")?;
            match result {
                ResultSet::Insert { count } => {
                    assert_eq!(count, 1);
                }
                _ => panic!("Expected Insert result"),
            }

            // Query data
            let result = session.execute("SELECT * FROM test_table;")?;
            println!("Result: {:?}", result);
            if let ResultSet::Scan { columns: _, rows } = result {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows, create_test_rows());
            } else {
                panic!("Expected Scan result");
            }

            Ok(())
        }

        pub fn run_sql_error_tests<E: storage::Engine + 'static>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let session = kv_engine.session()?;

            // Create table
            session.execute(
                "CREATE TABLE test_table (id INT PRIMARY KEY NOT NULL, name TEXT, age INT);",
            )?;

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
        pub fn run_primary_key_tests<E: storage::Engine + 'static>(engine: E) -> Result<()> {
            let kv_engine = KVEngine::new(engine);
            let mut txn = kv_engine.begin()?;

            // Create test table
            let table = create_test_table("test_pk_table");
            txn.create_table(table.clone())?;

            // Insert first row with primary key 1
            let row1 = vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::Integer(30),
            ];
            txn.create_row(table.name.clone(), row1)?;

            // Try inserting another row with the same primary key
            let row1_duplicate = vec![
                Value::Integer(1), // Same primary key as row1
                Value::String("Different Name".to_string()),
                Value::Integer(40),
            ];

            // This should fail with a proper error message about duplicate primary key
            let result = txn.create_row(table.name.clone(), row1_duplicate);
            assert!(result.is_err());
            if let Err(Error::InternalError(msg)) = result {
                assert!(msg.contains("Duplicated data for primary key"));
                assert!(msg.contains("already exists in table"));
            } else {
                panic!("Expected InternalError for duplicate primary key");
            }

            // Test with SQL execution
            txn.commit()?;

            let session = kv_engine.session()?;

            // Create a new test table using SQL
            session
                .execute("CREATE TABLE sql_pk_test (id INT PRIMARY KEY NOT NULL, name TEXT);")?;

            // Insert first row
            session.execute("INSERT INTO sql_pk_test VALUES (10, 'First');")?;

            // Try inserting duplicate primary key
            let result = session.execute("INSERT INTO sql_pk_test VALUES (10, 'Second');");
            assert!(result.is_err());
            if let Err(Error::InternalError(msg)) = result {
                assert!(msg.contains("Duplicated data for primary key"));
            } else {
                panic!("Expected InternalError for duplicate primary key in SQL execution");
            }

            // Test table without primary key
            let invalid_table = Table {
                name: "no_pk_table".to_string(),
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        datatype: DataType::Integer,
                        nullable: false,
                        default: None,
                        primary_key: false, // No primary key!
                    },
                    Column {
                        name: "name".to_string(),
                        datatype: DataType::String,
                        nullable: true,
                        default: Some(Value::Null),
                        primary_key: false,
                    },
                ],
            };

            let result = txn.create_table(invalid_table);
            assert!(result.is_err());
            if let Err(Error::InternalError(msg)) = result {
                assert!(msg.contains("has no primary key"));
            } else {
                panic!("Expected InternalError for table without primary key");
            }

            // Test table with multiple primary keys
            let multi_pk_table = Table {
                name: "multi_pk_table".to_string(),
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        datatype: DataType::Integer,
                        nullable: false,
                        default: None,
                        primary_key: true, // First primary key
                    },
                    Column {
                        name: "name".to_string(),
                        datatype: DataType::String,
                        nullable: true,
                        default: Some(Value::Null),
                        primary_key: true, // Second primary key
                    },
                ],
            };

            let result = txn.create_table(multi_pk_table);
            assert!(result.is_err());
            if let Err(Error::InternalError(msg)) = result {
                assert!(msg.contains("more than one primary key"));
            } else {
                panic!("Expected InternalError for table with multiple primary keys");
            }

            // Verify through SQL as well
            let result = session
                .execute("CREATE TABLE bad_pk_table (id INT PRIMARY KEY, name TEXT PRIMARY KEY);");
            assert!(result.is_err());

            Ok(())
        }
    }
}
