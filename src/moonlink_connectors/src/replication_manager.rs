use std::collections::HashMap;

use crate::{PostgresSourceError, ReplicationConnection};
use moonlink::{IcebergSnapshotStateManager, ReadStateManager};

/// Manage replication sources keyed by their connection URI.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager {
    connections: HashMap<String, ReplicationConnection>,
    table_base_path: String,
}

impl ReplicationManager {
    pub fn new(table_base_path: String) -> Self {
        Self {
            connections: HashMap::new(),
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
        if !self.connections.contains_key(uri) {
            // Lazily create the directory that will hold all tables.
            // This will not overwrite any existing directory.
            tokio::fs::create_dir_all(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let base_path = tokio::fs::canonicalize(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let replication_connection =
                ReplicationConnection::new(uri.to_owned(), base_path.to_str().unwrap().to_string())
                    .await?;
            self.connections
                .insert(uri.to_string(), replication_connection);
        }
        let replication_connection = self.connections.get_mut(uri).unwrap();
        replication_connection.add_table(table_name).await
    }
}
