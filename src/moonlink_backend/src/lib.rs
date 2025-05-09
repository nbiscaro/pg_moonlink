pub use moonlink::ReadState;
use moonlink::ReadStateManager; // ReadStateManager.try_read is a key method
mod error;
mod profiling;

pub use error::Error;
use error::Result;
use moonlink_connectors::MoonlinkPostgresSource; // MoonlinkPostgresSource is already profiled
use std::sync::Arc;
use std::{collections::HashMap, hash::Hash};
use tokio::sync::RwLock;

// Add ProfileGuard
use crate::profiling::ProfileGuard;

pub struct MoonlinkBackend<T: Eq + Hash> {
    ingest_sources: RwLock<Vec<MoonlinkPostgresSource>>,
    table_readers: RwLock<HashMap<T, ReadStateManager>>,
}

impl<T: Eq + Hash> Default for MoonlinkBackend<T> {
    fn default() -> Self {
        // Self::new() is very cheap, no need to profile default() itself.
        Self::new()
    }
}

impl<T: Eq + Hash> MoonlinkBackend<T> {
    pub fn new() -> Self {
        // new() is just initializing RwLocks with empty collections, very cheap.
        // No profiling needed here unless these RwLock creations themselves are suspect
        // under extreme contention, which is unlikely for `new`.
        Self {
            ingest_sources: RwLock::new(Vec::new()),
            table_readers: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_table(&self, table_id: T, table: &str, uri: &str) -> Result<()> {
        let _guard = ProfileGuard::new(&format!(
            "BACKEND_create_table_id_{:?}_table_{}_uri_{}", // Assuming T is Debug for the tag
            "id", table, uri
        ));

        let mut ingest_sources_lock = {
            let _lock_guard =
                ProfileGuard::new("BACKEND_create_table_ingest_sources_write_lock_acquire");
            self.ingest_sources.write().await
        };

        for ingest_source in ingest_sources_lock.iter_mut() {
            if ingest_source.check_table_belongs_to_source(uri) {
                // check_table_belongs_to_source is cheap
                // ingest_source.add_table is already profiled internally.
                // This guard shows the time for add_table specifically in this "existing source" path.
                let reader_state_manager_val = {
                    // Renamed to avoid conflict
                    let _add_table_guard = ProfileGuard::new(&format!(
                        "BACKEND_create_table_existing_source_add_table_{}",
                        table
                    ));
                    ingest_source.add_table(table).await?
                };
                {
                    let _readers_lock_guard = ProfileGuard::new(
                        "BACKEND_create_table_table_readers_write_lock_acquire_existing_source",
                    );
                    let mut table_readers_lock = self.table_readers.write().await;
                    let _insert_guard = ProfileGuard::new(
                        "BACKEND_create_table_table_readers_insert_existing_source",
                    );
                    table_readers_lock.insert(table_id, reader_state_manager_val);
                }
                return Ok(());
            }
        }

        // If we reach here, no existing source matched. Create a new one.
        // MoonlinkPostgresSource::new is already profiled internally.
        let mut new_ingest_source = {
            // Renamed to avoid conflict
            let _new_source_guard =
                ProfileGuard::new(&format!("BACKEND_create_table_new_source_new_uri_{}", uri));
            MoonlinkPostgresSource::new(uri.to_owned()).await?
        };

        // new_ingest_source.add_table is already profiled internally.
        let reader_state_manager_val_new = {
            // Renamed to avoid conflict
            let _add_table_new_source_guard = ProfileGuard::new(&format!(
                "BACKEND_create_table_new_source_add_table_{}",
                table
            ));
            new_ingest_source.add_table(table).await?
        };

        // Pushing to vec is cheap.
        ingest_sources_lock.push(new_ingest_source);

        {
            let _readers_lock_guard_new = ProfileGuard::new(
                "BACKEND_create_table_table_readers_write_lock_acquire_new_source",
            );
            let mut table_readers_lock_new = self.table_readers.write().await;
            let _insert_guard_new =
                ProfileGuard::new("BACKEND_create_table_table_readers_insert_new_source");
            table_readers_lock_new.insert(table_id, reader_state_manager_val_new);
        }
        Ok(())
    }

    pub async fn drop_table(&self, _table_id: T) -> Result<()> {
        // Not profiling todo!()
        todo!()
    }

    pub async fn scan_table(&self, table_id: &T, lsn: Option<u64>) -> Result<Arc<ReadState>> {
        let _guard = ProfileGuard::new(&format!(
            "BACKEND_scan_table_id_{:?}_lsn_{:?}", // Assuming T is Debug
            "id", lsn
        ));

        let table_readers_lock = {
            let _lock_guard =
                ProfileGuard::new("BACKEND_scan_table_table_readers_read_lock_acquire");
            self.table_readers.read().await
        };

        let reader = table_readers_lock
            .get(table_id)
            .expect("try to scan a table that does not exist"); // Get from HashMap is fast.

        // reader.try_read is the key Moonlink operation for reads.
        // This is where "Flush to disk and materialization of rows" might be indirectly triggered
        // if try_read needs to wait for new data to be materialized.
        // And it's explicitly the "Read with try_read" step.
        let read_state_val = {
            // Renamed to avoid conflict
            let _try_read_guard = ProfileGuard::new(&format!(
                "BACKEND_scan_table_reader_try_read_id_{:?}_lsn_{:?}", // Assuming T is Debug
                "id", lsn
            ));
            reader.try_read(lsn).await?
        };
        Ok(read_state_val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::{connect, NoTls};
    #[tokio::test]
    async fn test_moonlink_service() {
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let service = MoonlinkBackend::<&'static str>::new();
        // connect to postgres and create a table
        let (client, connection) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client.simple_query("DROP TABLE IF EXISTS test; CREATE TABLE test (id bigint PRIMARY KEY, name VARCHAR(255));").await.unwrap();
        println!("created table");
        service
            .create_table("test", "public.test", uri)
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test  VALUES (1 ,'foo');")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO test  VALUES (2 ,'bar');")
            .await
            .unwrap();
        let old = service.scan_table(&"test", None).await.unwrap();
        // wait 2 second
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let new = service.scan_table(&"test", None).await.unwrap();
        assert_ne!(old.data, new.data);
    }
}
