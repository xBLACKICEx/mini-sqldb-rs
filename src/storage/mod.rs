use crate::error::Result;

pub struct Mvcc {}

impl Clone for Mvcc {
    fn clone(&self) -> Self {
        Mvcc {}
    }
}

impl Mvcc {
    pub fn new() -> Mvcc {
        Mvcc {}
    }

    pub fn begin(&self) -> Result<MvccTransaction> {
        Ok(MvccTransaction {})
    }
}

pub struct MvccTransaction {}

impl MvccTransaction {
    pub fn new() -> MvccTransaction {
        MvccTransaction {}
    }
}
