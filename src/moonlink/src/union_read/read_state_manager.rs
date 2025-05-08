use crate::error::Error;
use crate::error::Result;
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
            last_read_state: RwLock::new(Arc::new(ReadState::new((vec![], vec![]), vec![]))),
            table_snapshot,
            table_snapshot_watch_receiver,
            replication_lsn_rx,
            table_commit_lsn_rx,
        }
    }

    /// Attempts to read state at or after the specified LSN.
    /// If `lsn` is `None`, it attempts to read the latest available state.
    pub async fn try_read(&self, requested_lsn: Option<u64>) -> Result<Arc<ReadState>> {
        // 1. Early exit: If a specific LSN is requested and it's older than our last read,
        //    return the cached state.
        if let Some(req_lsn_val) = requested_lsn {
            if req_lsn_val < self.last_read_lsn.load(Ordering::Relaxed) {
                let last_state = self.last_read_state.read().await;
                return Ok(last_state.clone());
            }
        }

        let mut table_snapshot_rx = self.table_snapshot_watch_receiver.clone();
        let mut replication_lsn_rx = self.replication_lsn_rx.clone();
        let table_commit_lsn_rx = self.table_commit_lsn_rx.clone();

        loop {
            let current_snapshot_lsn = *table_snapshot_rx.borrow();
            let current_replication_lsn = *replication_lsn_rx.borrow();
            let current_commit_lsn = *table_commit_lsn_rx.borrow();

            if self.can_satisfy_read_from_snapshot(
                requested_lsn,
                current_snapshot_lsn,
                current_replication_lsn,
                current_commit_lsn,
            ) {
                // If conditions are met, attempt to read from the snapshot and update cache.
                return self
                    .read_from_snapshot_and_update_cache(
                        current_snapshot_lsn,
                        current_replication_lsn,
                        current_commit_lsn,
                    )
                    .await;
            }

            // If conditions are not met, wait for relevant LSNs to change.
            self.wait_for_relevant_lsn_change(
                requested_lsn.unwrap(),
                current_replication_lsn,
                &mut replication_lsn_rx,
                &mut table_snapshot_rx,
            )
            .await?;
        }
    }

    /// Checks if the read request can be satisfied with the current LSNs.
    fn can_satisfy_read_from_snapshot(
        &self,
        requested_lsn: Option<u64>,
        snapshot_lsn: u64,
        replication_lsn: u64,
        commit_lsn: u64,
    ) -> bool {
        match requested_lsn {
            // If no specific LSN is requested, we can always try to read the latest.
            None => true,
            Some(req_lsn_val) => {
                // Request can be satisfied if:
                // 1. The requested LSN is already covered by the table snapshot.
                // OR
                // 2. The requested LSN is covered by replication, AND the snapshot
                //    reflects all committed changes up to the snapshot LSN.
                req_lsn_val <= snapshot_lsn
                    || (req_lsn_val <= replication_lsn && snapshot_lsn == commit_lsn)
            }
        }
    }

    /// Reads from the table snapshot, updates the internal cache, and returns the read state.
    async fn read_from_snapshot_and_update_cache(
        &self,
        current_snapshot_lsn: u64,
        current_replication_lsn: u64,
        current_commit_lsn: u64,
    ) -> Result<Arc<ReadState>> {
        // Acquire locks: read lock for table_snapshot, write lock for last_read_state.
        // Order matters if other parts of the code acquire these locks in a specific order.
        let table_state_snapshot = self.table_snapshot.read().await;
        let mut last_read_state_guard = self.last_read_state.write().await;

        // Check if another thread updated the cache to a sufficient LSN while we were waiting for locks.
        // Or, if our current snapshot is indeed newer than what's cached.
        if self.last_read_lsn.load(Ordering::Acquire) < current_snapshot_lsn {
            // Determine the effective LSN for this read.
            // If the snapshot is fully committed and replication has progressed further,
            // we can consider the state valid up to the replication LSN.
            let effective_lsn = if current_snapshot_lsn == current_commit_lsn
                && current_snapshot_lsn < current_replication_lsn
            {
                current_replication_lsn
            } else {
                current_snapshot_lsn
            };

            let read_output = table_state_snapshot.request_read()?;

            self.last_read_lsn.store(effective_lsn, Ordering::Release);
            *last_read_state_guard = Arc::new(ReadState::new(
                (read_output.file_paths, read_output.deletions),
                read_output.associated_files,
            ));
        }
        // Return the (potentially updated) cached state.
        Ok(last_read_state_guard.clone())
    }

    /// Waits for either the replication LSN or table snapshot LSN to change.
    async fn wait_for_relevant_lsn_change(
        &self,
        requested_lsn_val: u64,
        current_replication_lsn: u64,
        replication_lsn_rx: &mut watch::Receiver<u64>,
        table_snapshot_rx: &mut watch::Receiver<u64>,
    ) -> Result<()> {
        // If requested LSN is beyond current replication LSN, wait for replication.
        // Otherwise, wait for the table snapshot to catch up.
        if requested_lsn_val > current_replication_lsn {
            replication_lsn_rx
                .changed()
                .await
                .map_err(|e| Error::WatchChannelRecvError { source: e })?;
        } else {
            table_snapshot_rx
                .changed()
                .await
                .map_err(|e| Error::WatchChannelRecvError { source: e })?;
        }
        Ok(())
    }
}
