use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, InfallibleSinkError},
        PipelineReplicationState, PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use crate::postgres::util::postgres_schema_to_moonlink_schema;
use crate::postgres::util::PostgresTableRow;
use async_trait::async_trait;
use moonlink::ReadStateManager;
use moonlink::{MooncakeTable, TableEvent, TableHandler}; // TableEvent, TableHandler are key
use std::collections::{HashMap, HashSet};
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

// Assuming ProfileGuard is in src/profiling.rs (and now uses println!)
use crate::profiling::ProfileGuard;

#[derive(Default)]
struct TransactionState {
    final_lsn: u64,
    touched_tables: HashSet<TableId>,
}

pub struct Sink {
    table_handlers: HashMap<TableId, TableHandler>, // These are the Moonlink handlers
    last_committed_lsn_per_table: HashMap<TableId, watch::Sender<u64>>,
    event_senders: HashMap<TableId, Sender<TableEvent>>, // Senders to TableHandlers
    streaming_transactions_state: HashMap<u32, TransactionState>,
    transaction_state: TransactionState,
    reader_notifier: Sender<ReadStateManager>,
    base_path: PathBuf,
    replication_state: Arc<PipelineReplicationState>,
}

impl Sink {
    pub fn new(reader_notifier: Sender<ReadStateManager>, base_path: PathBuf) -> Self {
        // new() is mostly setup, likely cheap. No profiling for now.
        let event_senders = HashMap::new();
        Self {
            table_handlers: HashMap::new(),
            last_committed_lsn_per_table: HashMap::new(),
            event_senders,
            streaming_transactions_state: HashMap::new(),
            transaction_state: TransactionState {
                final_lsn: 0,
                touched_tables: HashSet::new(),
            },
            reader_notifier,
            base_path,
            replication_state: PipelineReplicationState::new(),
        }
    }
}

#[async_trait]
impl BatchSink for Sink {
    type Error = InfallibleSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        let _guard = ProfileGuard::new("SINK_get_resumption_state");
        // Currently returns a default state, very fast.
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        let _guard = ProfileGuard::new(&format!(
            "SINK_write_table_schemas_count_{}",
            table_schemas.len()
        ));
        let table_handlers = &mut self.table_handlers;
        for (table_id, table_schema) in table_schemas {
            let _table_schema_setup_guard = ProfileGuard::new(&format!(
                "SINK_write_table_schemas_setup_table_{}_{}",
                table_id, table_schema.table_name
            ));

            let table_path =
                PathBuf::from(&self.base_path).join(table_schema.table_name.to_string());
            // create_dir_all can have I/O cost, though usually fast if dir exists
            {
                let _dir_guard = ProfileGuard::new(&format!(
                    "SINK_write_table_schemas_create_dir_{}",
                    table_schema.table_name
                ));
                create_dir_all(&table_path).unwrap();
            }

            let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(&table_schema);

            let table = {
                let _mooncake_new_guard = ProfileGuard::new(&format!(
                    "SINK_write_table_schemas_mooncake_new_{}",
                    table_schema.table_name
                ));
                MooncakeTable::new(
                    arrow_schema,
                    table_schema.table_name.to_string(),
                    table_id as u64,
                    table_path,
                    identity,
                    /*iceberg_table_config=*/ None,
                )
            };

            let (table_commit_tx, table_commit_rx) = watch::channel(0u64);
            self.last_committed_lsn_per_table
                .insert(table_id, table_commit_tx);

            let read_state_manager = {
                let _rsm_new_guard = ProfileGuard::new(&format!(
                    "SINK_write_table_schemas_read_state_manager_new_{}",
                    table_schema.table_name
                ));
                ReadStateManager::new(&table, self.replication_state.subscribe(), table_commit_rx)
            };

            let table_handler = {
                let _th_new_guard = ProfileGuard::new(&format!(
                    "SINK_write_table_schemas_table_handler_new_{}",
                    table_schema.table_name
                ));
                TableHandler::new(table) // This might start background tasks for the handler
            };

            self.event_senders
                .insert(table_id, table_handler.get_event_sender()); // get_event_sender is likely cheap clone

            {
                let _notify_guard = ProfileGuard::new(&format!(
                    "SINK_write_table_schemas_reader_notifier_send_{}",
                    table_schema.table_name
                ));
                self.reader_notifier.send(read_state_manager).await.unwrap(); // mpsc send
            }
            table_handlers.insert(table_id, table_handler);
        }
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        let _guard = ProfileGuard::new(&format!(
            "SINK_write_table_rows_table_{}_count_{}",
            table_id,
            rows.len()
        ));
        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
        for (idx, row) in rows.into_iter().enumerate() {
            // Use into_iter to consume rows
            let _send_guard = ProfileGuard::new(&format!(
                "SINK_write_table_rows_table_{}_send_append_idx_{}",
                table_id, idx
            ));
            // This send is to the TableHandler's mpsc channel. The time taken here is for the send operation,
            // not for the full processing by TableHandler. TableHandler processing is asynchronous.
            event_sender
                .send(TableEvent::Append {
                    row: PostgresTableRow(row).into(),
                    xact_id: None,
                })
                .await
                .unwrap();
        }

        {
            let _commit_send_guard = ProfileGuard::new(&format!(
                "SINK_write_table_rows_table_{}_send_commit",
                table_id
            ));
            event_sender
                .send(TableEvent::Commit { lsn: 0 }) // LSN 0 for initial copy
                .await
                .unwrap();
        }
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let _guard = ProfileGuard::new(&format!(
            "SINK_write_cdc_events_batch_count_{}",
            events.len()
        ));
        let mut last_processed_lsn = PgLsn::from(0); // Or derive from events
        for event in events {
            last_processed_lsn = self.write_cdc_event(event).await?; // write_cdc_event is profiled
        }
        Ok(last_processed_lsn)
    }

    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, Self::Error> {
        // Dynamic tag based on event type

        match event {
            CdcEvent::Begin(begin_body) => {
                // This is state management, very fast.
                self.transaction_state.final_lsn = begin_body.final_lsn();
            }
            CdcEvent::StreamStart(_stream_start_body) => { /* Fast */ }
            CdcEvent::Commit(commit_body) => {
                let _commit_processing_guard = ProfileGuard::new(&format!(
                    "SINK_cdc_event_commit_lsn_{}_tables_{}",
                    commit_body.commit_lsn(),
                    self.transaction_state.touched_tables.len()
                ));
                for table_id in &self.transaction_state.touched_tables {
                    let event_sender = self.event_senders.get_mut(table_id).unwrap();
                    {
                        let _send_event_guard = ProfileGuard::new(&format!(
                            "SINK_cdc_event_commit_send_table_{}",
                            table_id
                        ));
                        event_sender
                            .send(TableEvent::Commit {
                                lsn: commit_body.commit_lsn(),
                            })
                            .await
                            .unwrap();
                    }
                    {
                        let _send_watch_guard = ProfileGuard::new(&format!(
                            "SINK_cdc_event_commit_watch_send_table_{}",
                            table_id
                        ));
                        self.last_committed_lsn_per_table
                            .get_mut(table_id)
                            .unwrap()
                            .send(commit_body.commit_lsn()) // watch::Sender::send is usually fast
                            .unwrap();
                    }
                }
                self.transaction_state.touched_tables.clear(); // Fast
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                let xact_id = stream_commit_body.xid();
                let _stream_commit_processing_guard = ProfileGuard::new(&format!(
                    "SINK_cdc_event_stream_commit_xid_{}_lsn_{}",
                    xact_id,
                    stream_commit_body.commit_lsn()
                ));
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get_mut(table_id).unwrap();
                        {
                            let _send_event_guard = ProfileGuard::new(&format!(
                                "SINK_cdc_event_stream_commit_send_table_{}",
                                table_id
                            ));
                            event_sender
                                .send(TableEvent::StreamCommit {
                                    lsn: stream_commit_body.commit_lsn(),
                                    xact_id,
                                })
                                .await
                                .unwrap();
                        }
                        {
                            let _send_watch_guard = ProfileGuard::new(&format!(
                                "SINK_cdc_event_stream_commit_watch_send_table_{}",
                                table_id
                            ));
                            self.last_committed_lsn_per_table
                                .get_mut(table_id)
                                .unwrap()
                                .send(stream_commit_body.commit_lsn())
                                .unwrap();
                        }
                    }
                }
                self.streaming_transactions_state.remove(&xact_id); // Fast
            }
            CdcEvent::Insert((table_id, table_row, xact_id)) => {
                let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                event_sender
                    .send(TableEvent::Append {
                        row: PostgresTableRow(table_row).into(),
                        xact_id,
                    })
                    .await
                    .unwrap();
                // State management below is fast
                if let Some(xid) = xact_id {
                    self.streaming_transactions_state
                        .entry(xid)
                        .or_default()
                        .touched_tables
                        .insert(table_id);
                } else {
                    self.transaction_state.touched_tables.insert(table_id);
                }
            }
            CdcEvent::Update((table_id, old_table_row, new_table_row, xact_id)) => {
                // State management first
                let final_lsn = if let Some(xid) = xact_id {
                    self.streaming_transactions_state
                        .entry(xid)
                        .or_default()
                        .touched_tables
                        .insert(table_id);
                    self.streaming_transactions_state
                        .get(&xid)
                        .unwrap()
                        .final_lsn
                } else {
                    self.transaction_state.touched_tables.insert(table_id);
                    self.transaction_state.final_lsn
                };

                let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                {
                    let _delete_send_guard = ProfileGuard::new(&format!(
                        "SINK_cdc_event_update_send_delete_table_{}_xid_{:?}",
                        table_id, xact_id
                    ));
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(old_table_row.unwrap()).into(),
                            lsn: final_lsn,
                            xact_id,
                        })
                        .await
                        .unwrap();
                }
                {
                    let _append_send_guard = ProfileGuard::new(&format!(
                        "SINK_cdc_event_update_send_append_table_{}_xid_{:?}",
                        table_id, xact_id
                    ));
                    event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(new_table_row).into(),
                            xact_id,
                        })
                        .await
                        .unwrap();
                }
            }
            CdcEvent::Delete((table_id, table_row, xact_id)) => {
                // State management first
                let final_lsn = if let Some(xid) = xact_id {
                    self.streaming_transactions_state
                        .entry(xid)
                        .or_default()
                        .touched_tables
                        .insert(table_id);
                    self.streaming_transactions_state
                        .get(&xid)
                        .unwrap()
                        .final_lsn
                } else {
                    self.transaction_state.touched_tables.insert(table_id);
                    self.transaction_state.final_lsn
                };

                let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                {
                    let _delete_send_guard = ProfileGuard::new(&format!(
                        "SINK_cdc_event_delete_send_table_{}_xid_{:?}",
                        table_id, xact_id
                    ));
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(table_row).into(),
                            lsn: final_lsn,
                            xact_id,
                        })
                        .await
                        .unwrap();
                }
            }
            CdcEvent::Relation(relation_body) => {
                // println! is fine, no perf impact typically from just this.
                println!("Relation {relation_body:?}");
            }
            CdcEvent::Type(type_body) => {
                println!("Type {type_body:?}");
            }
            CdcEvent::PrimaryKeepAlive(primary_keepalive_body) => {
                // This is fast.
                self.replication_state
                    .mark(PgLsn::from(primary_keepalive_body.wal_end()));
            }
            CdcEvent::StreamStop(_stream_stop_body) => { /* Fast */ }
            CdcEvent::StreamAbort(stream_abort_body) => {
                let xact_id = stream_abort_body.xid();
                let _stream_abort_processing_guard =
                    ProfileGuard::new(&format!("SINK_cdc_event_stream_abort_xid_{}", xact_id));
                if let Some(tables_in_txn) = self.streaming_transactions_state.get(&xact_id) {
                    for table_id in &tables_in_txn.touched_tables {
                        let event_sender = self.event_senders.get_mut(table_id).unwrap();
                        {
                            let _send_event_guard = ProfileGuard::new(&format!(
                                "SINK_cdc_event_stream_abort_send_table_{}",
                                table_id
                            ));
                            event_sender
                                .send(TableEvent::StreamAbort { xact_id })
                                .await
                                .unwrap();
                        }
                    }
                }
                self.streaming_transactions_state.remove(&xact_id); // Fast
            }
        }
        // The LSN returned here is meant to be the LSN *processed by the sink*.
        // The current logic returns a static 0, which might not be correct for resumption.
        // It should probably be derived from the event (e.g., commit_lsn, final_lsn).
        // For profiling purposes, this return value itself isn't timed.
        Ok(PgLsn::from(self.transaction_state.final_lsn)) // Example: use final_lsn from current txn
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        let _guard = ProfileGuard::new(&format!("SINK_table_copied_table_{}", table_id));
        println!("table {table_id} copied"); // Original println
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        let _guard = ProfileGuard::new(&format!("SINK_truncate_table_table_{}", table_id));
        println!("table {table_id} truncated"); // Original println
                                                // Note: This sink doesn't actually send a Truncate event to TableHandler.
                                                // If truncation needs to be reflected in Moonlink, a TableEvent::Truncate would be needed.
        Ok(())
    }
}
