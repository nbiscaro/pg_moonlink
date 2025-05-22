use crate::pg_replicate::conversions::cdc_event::{CdcEvent, CdcEventConversionError};
use crate::pg_replicate::moonlink_sink::Sink;
use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSource, PostgresSourceError, TableNamesFrom,
};
use crate::pg_replicate::table_init::build_table_components;
use moonlink::{IcebergSnapshotStateManager, ReadStateManager};
use tokio::pin;

use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::TableId;
use futures::StreamExt;
use std::collections::HashMap;
use std::path::Path;
use tokio::task::JoinHandle;
use tokio_postgres::{connect, Client, NoTls};
pub struct ReplicationConnection {
    uri: String,
    table_base_path: String,
    postgres_client: Client,
    handle: Option<JoinHandle<Result<(), PostgresSourceError>>>,
    table_readers: HashMap<TableId, ReadStateManager>,
    iceberg_snapshot_managers: HashMap<TableId, IcebergSnapshotStateManager>,
}

impl ReplicationConnection {
    pub async fn new(uri: String, table_base_path: String) -> Result<Self, PostgresSourceError> {
        let (postgres_client, connection) = connect(&uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });
        postgres_client
            .simple_query(
                "DROP PUBLICATION IF EXISTS moonlink_pub; CREATE PUBLICATION moonlink_pub;",
            )
            .await
            .unwrap();
        Ok(Self {
            uri,
            table_base_path,
            postgres_client,
            handle: None,
            table_readers: HashMap::new(),
            iceberg_snapshot_managers: HashMap::new(),
        })
    }

    async fn add_table_to_publication(&self, table_name: &str) -> Result<(), PostgresSourceError> {
        self.postgres_client
            .simple_query(&format!(
                "ALTER PUBLICATION moonlink_pub ADD TABLE {};",
                table_name
            ))
            .await
            .unwrap();
        self.postgres_client
            .simple_query(&format!(
                "ALTER TABLE {} REPLICA IDENTITY FULL;",
                table_name
            ))
            .await
            .unwrap();
        Ok(())
    }

    async fn create_postgres_source(&self) -> Result<PostgresSource, PostgresSourceError> {
        PostgresSource::new(
            &self.uri,
            Some("moonlink_slot".to_string()),
            TableNamesFrom::Publication("moonlink_pub".to_string()),
        )
        .await
    }

    pub fn get_table_reader(&self, table_id: TableId) -> &ReadStateManager {
        self.table_readers.get(&table_id).unwrap()
    }

    pub fn get_iceberg_snapshot_manager(
        &mut self,
        table_id: TableId,
    ) -> &mut IcebergSnapshotStateManager {
        self.iceberg_snapshot_managers.get_mut(&table_id).unwrap()
    }

    async fn spawn_replication(
        &mut self,
        source: PostgresSource,
        sink: Sink,
    ) -> JoinHandle<Result<(), PostgresSourceError>> {
        tokio::spawn(async move { run_replication(source, sink).await })
    }

    pub async fn build_and_register_table_components(
        &mut self,
        source: &PostgresSource,
        sink: &mut Sink,
        replication_state: &ReplicationState,
    ) {
        for (table_id, schema) in source.get_table_schemas() {
            let resources =
                build_table_components(schema, Path::new(&self.table_base_path), replication_state)
                    .await;

            self.table_readers
                .insert(*table_id, resources.read_state_manager);
            self.iceberg_snapshot_managers
                .insert(*table_id, resources.iceberg_snapshot_manager);

            sink.register_table(*table_id, resources.sink_components);
        }
    }

    pub async fn add_table(&mut self, table_name: &str) -> Result<TableId, PostgresSourceError> {
        self.add_table_to_publication(table_name).await?;

        // TODO: We still build the replication connection as part of the add table functionality. The reason being we don't yet have a way to add a table to an already running replication connection. This is to be done in a follow up PR to support multiple tables.
        let replication_state = ReplicationState::new();
        let source = self.create_postgres_source().await?;
        let mut sink = Sink::new(replication_state.clone());
        let table_schemas = source.get_table_schemas().clone();
        self.build_and_register_table_components(&source, &mut sink, &replication_state)
            .await;

        let handle = self.spawn_replication(source, sink).await;
        self.handle = Some(handle);

        // TODO: This is temporary since we are still adding all the tables at the same time and then starting the replication. In the future, we will be able to add a new table to an already running replication connection in which case we want to return just id of this new table. This is what we are simulating here.
        let table_id = table_schemas
            .iter()
            .find(|(_, s)| s.table_name.to_string() == table_name)
            .map(|(id, _)| *id)
            .expect("table schema not found");

        Ok(table_id)
    }

    pub fn check_table_belongs_to_source(&self, uri: &str) -> bool {
        self.uri == uri
    }
}

async fn run_replication(
    mut source: PostgresSource,
    mut sink: Sink,
) -> Result<(), PostgresSourceError> {
    //TODO: add separate stream for initial table copy.
    // This assumes the table is empty and we can naively begin copying rows immediatley. If a table already contains data, this is not the case. We will need to use the get_table_copy_stream method to get the initial rows and write them into a new table. In parallel, we will need to buffer the cdc events for this table until the initial rows are written. For simplicity, we will assume that the table is empty at this point, and add a new stream for initial table copy later.

    source.commit_transaction().await?;

    // TODO: start from the last lsn in replication state for recovery.
    // TODO: track tables copied in replication state.
    let mut last_lsn: u64 = 0;
    last_lsn += 1;
    let stream = source.get_cdc_stream(last_lsn.into()).await?;

    pin!(stream);

    while let Some(event) = stream.next().await {
        let mut send_status_update = false;
        if let Err(CdcStreamError::CdcEventConversion(CdcEventConversionError::MissingSchema(_))) =
            &event
        {
            continue;
        }
        if let Ok(CdcEvent::PrimaryKeepAlive(body)) = &event {
            send_status_update = body.reply() == 1;
        }
        let event = event?;
        let last_lsn = sink.write_cdc_event(event).await.unwrap();
        if send_status_update {
            let _ = stream.as_mut().send_status_update(last_lsn).await;
        }
    }

    Ok(())
}
