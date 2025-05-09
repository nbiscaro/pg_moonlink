mod sink;
mod util;
use crate::pg_replicate::pipeline::{
    batching::data_pipeline::BatchDataPipeline, // BatchDataPipeline is already profiled
    batching::BatchConfig,
    sinks::InfallibleSinkError,
    sources::postgres::{PostgresSource, PostgresSourceError, TableNamesFrom}, // PostgresSource is already profiled
    PipelineAction,
    PipelineError,
};
use moonlink::ReadStateManager;

// Add ProfileGuard
use crate::profiling::ProfileGuard;
use sink::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, NoTls};

pub struct MoonlinkPostgresSource {
    uri: String,
    postgres_client: Client,
    handle: Option<
        JoinHandle<
            std::result::Result<(), PipelineError<PostgresSourceError, InfallibleSinkError>>,
        >,
    >,
}

impl MoonlinkPostgresSource {
    pub async fn new(uri: String) -> Result<Self, PostgresSourceError> {
        let _guard = ProfileGuard::new_with_prefix("MOONLINK_SRC_new", &uri);
        // The connect call can take time.
        let (postgres_client_val, connection) = {
            // Renamed to avoid conflict
            let _connect_guard = ProfileGuard::new_with_prefix("MOONLINK_SRC_new_pg_connect", &uri);
            connect(&uri, NoTls).await.unwrap()
        };
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });
        // The simple_query for publication setup can also take time.
        {
            let _pub_setup_guard = ProfileGuard::new("MOONLINK_SRC_new_pg_setup_publication");
            postgres_client_val
                .simple_query(
                    "DROP PUBLICATION IF EXISTS moonlink_pub; CREATE PUBLICATION moonlink_pub;",
                )
                .await
                .unwrap();
        }
        Ok(Self {
            uri,
            postgres_client: postgres_client_val,
            handle: None,
        })
    }

    pub async fn add_table(
        &mut self,
        table: &str,
    ) -> Result<ReadStateManager, PostgresSourceError> {
        let _guard = ProfileGuard::new_with_prefix("MOONLINK_SRC_add_table", table);
        // Each simple_query can take time.
        {
            let _alter_pub_guard = ProfileGuard::new(&format!(
                "MOONLINK_SRC_add_table_alter_publication_{}",
                table
            ));
            self.postgres_client
                .simple_query(&format!(
                    "ALTER PUBLICATION moonlink_pub ADD TABLE {};",
                    table
                ))
                .await
                .unwrap();
        }
        {
            let _alter_table_guard = ProfileGuard::new(&format!(
                "MOONLINK_SRC_add_table_alter_table_replica_identity_{}",
                table
            ));
            self.postgres_client
                .simple_query(&format!("ALTER TABLE {} REPLICA IDENTITY FULL;", table))
                .await
                .unwrap();
        }

        // PostgresSource::new is already profiled internally.
        // This call creates a new source for potentially each table added, which might be intensive.
        let source_val = {
            // Renamed to avoid conflict
            let _source_new_guard = ProfileGuard::new(&format!(
                "MOONLINK_SRC_add_table_postgres_source_new_for_table_{}",
                table
            ));
            PostgresSource::new(
                &self.uri,
                Some("moonlink_slot".to_string()), // Slot name is hardcoded here
                TableNamesFrom::Publication("moonlink_pub".to_string()), // Publication name is hardcoded
            )
            .await?
        };

        let (reader_notifier, mut reader_notifier_receiver) = mpsc::channel(1); // mpsc channel setup is fast

        // Sink::new is mostly setup, profiled lightly within Sink itself.
        let sink_val = {
            // Renamed to avoid conflict
            let _sink_new_guard = ProfileGuard::new(&format!(
                "MOONLINK_SRC_add_table_sink_new_for_table_{}",
                table
            ));
            Sink::new(reader_notifier, PathBuf::from("./mooncake_test/")) // Path is hardcoded
        };

        let batch_config = BatchConfig::new(1000, Duration::from_secs(1)); // Config setup is fast

        // BatchDataPipeline::new is cheap (struct instantiation).
        let mut pipeline =
            BatchDataPipeline::new(source_val, sink_val, PipelineAction::CdcOnly, batch_config);

        // Spawning the pipeline. The `pipeline.start().await` is where the work happens,
        // and it's already profiled internally.
        // The act of tokio::spawn itself is fast.
        let _spawn_guard = ProfileGuard::new(&format!(
            "MOONLINK_SRC_add_table_spawn_pipeline_for_table_{}",
            table
        ));
        self.handle = Some(tokio::spawn(async move { pipeline.start().await }));

        // Waiting for the ReadStateManager from the Sink via the mpsc channel.
        // This measures how long it takes for the Sink to initialize and send the first RSM.
        let res_val = {
            // Renamed to avoid conflict
            let _recv_rsm_guard = ProfileGuard::new(&format!(
                "MOONLINK_SRC_add_table_receive_rsm_for_table_{}",
                table
            ));
            reader_notifier_receiver.recv().await
        };
        Ok(res_val.unwrap())
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        // This is a cheap comparison, no profiling needed.
        self.uri == uri
    }
}
