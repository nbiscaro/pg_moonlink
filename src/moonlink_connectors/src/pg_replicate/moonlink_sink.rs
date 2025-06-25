use crate::pg_replicate::util::PostgresTableRow;
use crate::pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    replication_state::ReplicationState,
    table::TableId,
};
use moonlink::TableEvent;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;
use tracing::{debug, warn};

#[derive(Default)]
struct TransactionState {
    final_lsn: u64,
    touched_tables: HashSet<TableId>,
}

pub struct Sink {
    event_senders: HashMap<TableId, Sender<TableEvent>>,
    commit_lsn_txs: HashMap<TableId, watch::Sender<u64>>,
    streaming_transactions_state: HashMap<u32, TransactionState>,
    transaction_state: TransactionState,
    replication_state: Arc<ReplicationState>,
    last_lsn: u64,
}

impl Sink {
    pub fn new(replication_state: Arc<ReplicationState>) -> Self {
        Self {
            event_senders: HashMap::new(),
            commit_lsn_txs: HashMap::new(),
            streaming_transactions_state: HashMap::new(),
            transaction_state: TransactionState {
                final_lsn: 0,
                touched_tables: HashSet::new(),
            },
            replication_state,
            last_lsn: 0,
        }
    }
}

impl Sink {
    pub fn add_table(
        &mut self,
        table_id: TableId,
        event_sender: Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
    ) {
        self.event_senders.insert(table_id, event_sender);
        self.commit_lsn_txs.insert(table_id, commit_lsn_tx);
    }
    pub fn drop_table(&mut self, table_id: TableId) {
        self.event_senders.remove(&table_id).unwrap();
        self.commit_lsn_txs.remove(&table_id).unwrap();
    }

    pub async fn process_cdc_event(&mut self, event: CdcEvent) -> Result<PgLsn, Infallible> {
        match event {
            CdcEvent::Commit(commit_body) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                println!("received commit event at: {:?}", now);
                println!("xid: {:?}", commit_body.commit_lsn());
            }
            CdcEvent::StreamCommit(stream_commit_body) => {
                // print out the current timestamp
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                println!("received stream commit event at: {:?}", now);
                println!("xid: {:?}", stream_commit_body.xid());
            }
            CdcEvent::PrimaryKeepAlive(primary_keepalive_body) => {
                self.replication_state
                    .mark(PgLsn::from(primary_keepalive_body.wal_end()));
                self.last_lsn = primary_keepalive_body.wal_end();
            }
            _ => {}
        }
        Ok(PgLsn::from(self.last_lsn))
    }
}
