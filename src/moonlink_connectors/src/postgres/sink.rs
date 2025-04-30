use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, InfallibleSinkError},
        PipelineReplicationState, PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use crate::postgres::util::table_schema_to_arrow_schema;
use crate::postgres::util::PostgresTableRow;
use async_trait::async_trait;
use moonlink::ReadStateManager;
use moonlink::{MooncakeTable, TableEvent, TableHandler};
use std::collections::{HashMap, HashSet};
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

pub struct Sink {
    table_handlers: HashMap<TableId, TableHandler>,
    last_committed_lsn_per_table: HashMap<TableId, watch::Sender<u64>>,
    event_senders: HashMap<TableId, Sender<TableEvent>>,
    transaction_tables: HashMap<u32, HashSet<TableId>>,
    reader_notifier: Sender<ReadStateManager>,
    base_path: PathBuf,
    replication_state: Arc<PipelineReplicationState>,
}

impl Sink {
    pub fn new(reader_notifier: Sender<ReadStateManager>, base_path: PathBuf) -> Self {
        let event_senders = HashMap::new();
        Self {
            table_handlers: HashMap::new(),
            last_committed_lsn_per_table: HashMap::new(),
            event_senders,
            transaction_tables: HashMap::new(),
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
        Ok(PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        let table_handlers = &mut self.table_handlers;
        for (table_id, table_schema) in table_schemas {
            let table_path =
                PathBuf::from(&self.base_path).join(table_schema.table_name.to_string());
            create_dir_all(&table_path).unwrap();
            let table = MooncakeTable::new(
                table_schema_to_arrow_schema(&table_schema),
                table_schema.table_name.to_string(),
                table_id as u64,
                table_path,
            );
            let (table_commit_tx, table_commit_rx) = watch::channel(0u64);
            self.last_committed_lsn_per_table
                .insert(table_id, table_commit_tx);
            let read_state_manager =
                ReadStateManager::new(&table, self.replication_state.subscribe(), table_commit_rx);
            let table_handler = TableHandler::new(table);
            self.event_senders
                .insert(table_id, table_handler.get_event_sender());
            self.reader_notifier.send(read_state_manager).await.unwrap();
            table_handlers.insert(table_id, table_handler);
        }
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        let event_sender = self.event_senders.get_mut(&table_id).unwrap();
        for row in rows {
            event_sender
                .send(TableEvent::Append {
                    row: PostgresTableRow(row).into(),
                    xact_id: None,
                })
                .await
                .unwrap();
        }
        event_sender
            .send(TableEvent::Commit { lsn: 0 })
            .await
            .unwrap();
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let mut lsn_in_transaction: Option<u64> = None;
        // Track tables modified within this specific batch for non-streamed commits
        let mut tables_in_batch: HashSet<TableId> = HashSet::new();

        for event in events {
            println!("Received CDC event: {:?}", event);
            match event {
                CdcEvent::Begin(begin_body) => {
                    lsn_in_transaction = Some(begin_body.final_lsn());
                }
                CdcEvent::StreamStart(stream_start_body) => {
                    println!("Stream start {stream_start_body:?}");
                }
                CdcEvent::Commit(commit_body) => {
                    // Non-streamed commit: affects tables modified within this batch.
                    println!(
                        "PostgresSink: Processing non-streamed Commit with LSN: {}",
                        commit_body.commit_lsn()
                    );
                    for table_id in &tables_in_batch {
                        let event_sender = self.event_senders.get(table_id).unwrap().clone();
                        event_sender
                            .send(TableEvent::Commit {
                                lsn: commit_body.commit_lsn(),
                            })
                            .await
                            .unwrap();
                        self.last_committed_lsn_per_table
                            .get_mut(table_id)
                            .unwrap()
                            .send(commit_body.commit_lsn())
                            .unwrap();
                    }
                    tables_in_batch.clear(); // Clear for the next potential non-streamed TX in the batch
                }
                CdcEvent::StreamCommit(stream_commit_body) => {
                    // Note: Logic to populate self.transaction_tables moved to Insert/Update/Delete arms
                    let xact_id = stream_commit_body.xid();
                    if let Some(tables_in_txn) = self.transaction_tables.get(&xact_id) {
                        for table_id in tables_in_txn {
                            println!("PostgresSink: Sending TableEvent::StreamCommit for table_id={}, xact_id={}, lsn={}", 
                                table_id, xact_id, stream_commit_body.commit_lsn());
                            let event_sender = self.event_senders.get_mut(table_id).unwrap();
                            event_sender
                                .send(TableEvent::StreamCommit {
                                    lsn: stream_commit_body.commit_lsn(),
                                    xact_id,
                                })
                                .await
                                .unwrap();
                            self.last_committed_lsn_per_table
                                .get_mut(table_id)
                                .unwrap()
                                .send(stream_commit_body.commit_lsn())
                                .unwrap();
                        }
                    }
                    self.transaction_tables.remove(&xact_id);
                }
                CdcEvent::Insert((table_id, table_row, xact_id_opt)) => {
                    // Track table for potential non-streamed commit within this batch
                    tables_in_batch.insert(table_id);

                    let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                    event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(table_row).into(),
                            xact_id: xact_id_opt,
                        })
                        .await
                        .unwrap();
                    // Add to self.transaction_tables if this is part of a streamed transaction
                    if let Some(xid) = xact_id_opt {
                        self.transaction_tables
                            .entry(xid)
                            .or_default()
                            .insert(table_id);
                    }
                }
                CdcEvent::Update((table_id, old_table_row, new_table_row, xact_id_opt)) => {
                    // Track table for potential non-streamed commit within this batch
                    tables_in_batch.insert(table_id);

                    let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(old_table_row.unwrap()).into(),
                            lsn: (lsn_in_transaction.unwrap()),
                            xact_id: xact_id_opt,
                        })
                        .await
                        .unwrap();
                    event_sender
                        .send(TableEvent::Append {
                            row: PostgresTableRow(new_table_row).into(),
                            xact_id: xact_id_opt,
                        })
                        .await
                        .unwrap();
                    // Add to self.transaction_tables if this is part of a streamed transaction
                    if let Some(xid) = xact_id_opt {
                        self.transaction_tables
                            .entry(xid)
                            .or_default()
                            .insert(table_id);
                    }
                }
                CdcEvent::Delete((table_id, table_row, xact_id_opt)) => {
                    // Track table for potential non-streamed commit within this batch
                    tables_in_batch.insert(table_id);

                    let event_sender = self.event_senders.get_mut(&table_id).unwrap();
                    event_sender
                        .send(TableEvent::Delete {
                            row: PostgresTableRow(table_row).into(),
                            lsn: (lsn_in_transaction.unwrap()),
                            xact_id: xact_id_opt,
                        })
                        .await
                        .unwrap();
                    // Add to self.transaction_tables if this is part of a streamed transaction
                    if let Some(xid) = xact_id_opt {
                        self.transaction_tables
                            .entry(xid)
                            .or_default()
                            .insert(table_id);
                    }
                }
                CdcEvent::Relation(relation_body) => println!("Relation {relation_body:?}"),
                CdcEvent::Type(type_body) => println!("Type {type_body:?}"),
                CdcEvent::PrimaryKeepAlive(primary_keepalive_body) => {
                    self.replication_state
                        .mark(PgLsn::from(primary_keepalive_body.wal_end()));
                }
                CdcEvent::StreamStop(stream_stop_body) => {
                    println!("Stream stop {stream_stop_body:?}");
                }
                CdcEvent::StreamAbort(stream_abort_body) => {
                    let xact_id = stream_abort_body.xid();
                    // Get the tables involved *before* removing the entry
                    if let Some(tables_in_txn) = self.transaction_tables.get(&xact_id) {
                        // Collect table IDs to avoid borrowing issues while sending
                        let table_ids_to_abort: Vec<TableId> =
                            tables_in_txn.iter().cloned().collect();

                        for table_id in tables_in_txn {
                            // Send abort event - clone sender to avoid borrow issues
                            let event_sender = self.event_senders.get(table_id).unwrap().clone();
                            event_sender
                                .send(TableEvent::StreamAbort { xact_id })
                                .await
                                .unwrap();
                        }
                    }
                    // Now remove the transaction entry
                    self.transaction_tables.remove(&xact_id);
                }
            }
        }
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        println!("table {table_id} copied");
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        println!("table {table_id} truncated");
        Ok(())
    }
}
