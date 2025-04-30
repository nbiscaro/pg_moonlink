use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::union_read::read_state::ReadState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

pub struct ReadStateManager {
    last_read_lsn: AtomicU64,
    last_read_state: RwLock<Arc<ReadState>>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    replication_lsn_rx: watch::Receiver<u64>,
    table_commit_lsn_rx: watch::Receiver<u64>,
}

impl ReadStateManager {
    pub fn new(
        table: &MooncakeTable,
        replication_lsn_rx: watch::Receiver<u64>,
        table_commit_lsn_rx: watch::Receiver<u64>,
    ) -> Self {
        let (table_snapshot, table_snapshot_watch_receiver) = table.get_state_for_reader();
        ReadStateManager {
            last_read_lsn: AtomicU64::new(0),
            last_read_state: RwLock::new(Arc::new(ReadState::new((vec![], vec![])))),
            table_snapshot,
            table_snapshot_watch_receiver,
            replication_lsn_rx,
            table_commit_lsn_rx,
        }
    }

    /// Read after a specific lsn
    pub async fn try_read(&self, lsn: Option<u64>) -> Arc<ReadState> {
        if lsn.is_some() && lsn.unwrap() < self.last_read_lsn.load(Ordering::Relaxed) {
            let last_state = self.last_read_state.read().await;
            return last_state.clone();
        }
        let mut table_snapshot_watch_receiver = self.table_snapshot_watch_receiver.clone();
        let mut replication_lsn_rx = self.replication_lsn_rx.clone();
        let mut table_commit_lsn_rx = self.table_commit_lsn_rx.clone();

        loop {
            let current_table_snapshot_lsn = *table_snapshot_watch_receiver.borrow();
            let current_replication_lsn = *replication_lsn_rx.borrow();
            let current_table_commit_lsn = *table_commit_lsn_rx.borrow();

            let should_read_now = match lsn {
                None => true,
                Some(target) => {
                    if target <= current_table_snapshot_lsn {
                        true
                    } else if target <= current_replication_lsn
                        && current_table_snapshot_lsn == current_table_commit_lsn
                    {
                        true
                    } else {
                        false
                    }
                }
            };

            if should_read_now {
                let table_state = self.table_snapshot.read().await;
                let mut last_state = self.last_read_state.write().await;
                let last_read_lsn_before_update = self.last_read_lsn.load(Ordering::Acquire);

                if last_read_lsn_before_update < current_table_snapshot_lsn {
                    let effective_lsn = if current_table_snapshot_lsn == current_table_commit_lsn
                        && current_table_snapshot_lsn < current_replication_lsn
                    {
                        current_replication_lsn
                    } else {
                        current_table_snapshot_lsn
                    };
                    let ret = table_state.request_read().unwrap();

                    let formated = (
                        ret.0
                            .into_iter()
                            .map(|x| x.to_string_lossy().to_string())
                            .collect(),
                        ret.1
                            .into_iter()
                            .map(|x| (x.0 as u32, x.1 as u32))
                            .collect(),
                    );
                    self.last_read_lsn.store(effective_lsn, Ordering::Release);
                    *last_state = Arc::new(ReadState::new(formated));
                    return last_state.clone();
                } else {
                    return last_state.clone();
                }
            }

            match lsn {
                Some(target) if target > current_replication_lsn => {
                    replication_lsn_rx.changed().await.unwrap();
                }
                _ => {
                    table_snapshot_watch_receiver.changed().await.unwrap();
                }
            }
        }
    }
}
