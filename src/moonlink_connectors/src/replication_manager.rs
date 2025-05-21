use std::collections::HashMap;

use crate::{MoonlinkPostgresSource, PostgresSourceError};
use moonlink::{IcebergSnapshotStateManager, ReadStateManager};

/// Manage replication sources keyed by their connection URI.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager {
    sources: HashMap<String, MoonlinkPostgresSource>,
    table_base_path: String,
}

impl ReplicationManager {
    pub fn new(table_base_path: String) -> Self {
        Self {
            sources: HashMap::new(),
            table_base_path,
        }
    }

    /// Add a table to be replicated from the given `uri`.
    ///
    /// If replication for this `uri` is not yet running a new replication
    /// source will be created.
    pub async fn add_table(
        &mut self,
        uri: &str,
        table_name: &str,
    ) -> Result<(ReadStateManager, IcebergSnapshotStateManager), PostgresSourceError> {
        if !self.sources.contains_key(uri) {
            // Lazily create the directory that will hold all tables.
            // This will not overwrite any existing directory.
            tokio::fs::create_dir_all(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let base_path = tokio::fs::canonicalize(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let source = MoonlinkPostgresSource::new(
                uri.to_owned(),
                base_path.to_str().unwrap().to_string(),
            )
            .await?;
            self.sources.insert(uri.to_string(), source);
        }
        let source = self.sources.get_mut(uri).unwrap();
        source.add_table(table_name).await
    }
}
