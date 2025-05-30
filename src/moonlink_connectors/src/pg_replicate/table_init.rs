use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::TableSchema;
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use crate::{Error, Result};
use moonlink::{
    IcebergEventSyncReceiver, IcebergEventSyncSender, IcebergTableConfig, IcebergTableEventManager,
    MooncakeTable, ReadStateManager, TableConfig, TableEvent, TableHandler,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::{mpsc::Sender, watch};

/// Components required to replicate a single table.
/// Components that the [`Sink`] needs for processing CDC events.
pub struct TableComponents {
    pub event_sender: Sender<TableEvent>,
}

/// Resources that should be returned to the caller when a table is initialised.
pub struct TableResources {
    pub event_sender: Sender<TableEvent>,
    pub read_state_manager: ReadStateManager,
    pub iceberg_snapshot_manager: IcebergTableEventManager,
    pub commit_lsn_tx: watch::Sender<u64>,
    pub snapshot_lsn_rx: watch::Receiver<u64>,
}

/// Create iceberg table event manager sender and receiver.
fn create_iceberg_event_syncer() -> (IcebergEventSyncSender, IcebergEventSyncReceiver) {
    let (iceberg_snapshot_completion_tx, iceberg_snapshot_completion_rx) = mpsc::channel::<Result<u64>>(1);
    let (iceberg_drop_table_completion_tx, iceberg_drop_table_completion_rx) = mpsc::channel(1);
    let iceberg_event_sync_sender = IcebergEventSyncSender {
        iceberg_drop_table_completion_tx,
        iceberg_snapshot_completion_tx,
    };
    let iceberg_event_sync_receiver = IcebergEventSyncReceiver {
        iceberg_drop_table_completion_rx,
        iceberg_snapshot_completion_rx,
    };
    (iceberg_event_sync_sender, iceberg_event_sync_receiver)
}

/// Build all components needed to replicate `table_schema`.
pub async fn build_table_components(
    table_schema: &TableSchema,
    base_path: &Path,
    replication_state: &ReplicationState,
) -> Result<TableResources> {
    let table_path = PathBuf::from(base_path).join(table_schema.table_name.to_string());
    tokio::fs::create_dir_all(&table_path).await.unwrap();
    let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(table_schema);
    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri: base_path.to_str().unwrap().to_string(),
        namespace: vec!["default".to_string()],
        table_name: table_schema.table_name.to_string(),
        // TODO(hjiang): Disable recovery in production, at the moment we only support create new table from scratch.
        drop_table_if_exists: true,
    };
    let table = MooncakeTable::new(
        arrow_schema,
        table_schema.table_name.to_string(),
        table_schema.table_id as u64,
        table_path,
        identity,
        iceberg_table_config,
        TableConfig::new(),
    )
    .await?;

    let (commit_lsn_tx, commit_lsn_rx) = watch::channel(0u64);
    let (snapshot_lsn_tx, snapshot_lsn_rx) = watch::channel(0u64);
    let read_state_manager =
        ReadStateManager::new(&table, replication_state.subscribe(), commit_lsn_rx);
    let (iceberg_event_sync_sender, iceberg_event_sync_receiver) = create_iceberg_event_syncer();
    let handler = TableHandler::new(table, iceberg_event_sync_sender, snapshot_lsn_tx);
    let iceberg_snapshot_manager =
        IcebergTableEventManager::new(handler.get_event_sender(), iceberg_event_sync_receiver);
    let event_sender = handler.get_event_sender();

    Ok(TableResources {
        event_sender,
        read_state_manager,
        iceberg_snapshot_manager,
        commit_lsn_tx,
        snapshot_lsn_rx,
    })
}
