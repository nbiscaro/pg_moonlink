use crate::error::Result;
use crate::storage::mooncake_table::Snapshot;

// UNDONE(Iceberg):

pub trait IcebergSnapshot {
    // Write the current version to iceberg
    async fn _export_to_iceberg(&self) -> Result<()>;

    // Create a snapshot by reading from iceberg
    fn _load_from_iceberg(&self) -> Self;
}

impl IcebergSnapshot for Snapshot {
    async fn _export_to_iceberg(&self) -> Result<()> {
        todo!()
    }

    fn _load_from_iceberg(&self) -> Self {
        todo!()
    }
}
