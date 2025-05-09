use crate::error::Error;
use crate::error::Result;
use crate::storage::MooncakeTable; // Used in new() for get_state_for_reader
use crate::storage::SnapshotTableState; // Key for actual read
use crate::union_read::read_state::ReadState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

// Add ProfileGuard
use crate::profiling::ProfileGuard;

pub struct ReadStateManager {
    last_read_lsn: AtomicU64,
    last_read_state: RwLock<Arc<ReadState>>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>, // This is what we read from
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    replication_lsn_rx: watch::Receiver<u64>,
    table_commit_lsn_rx: watch::Receiver<u64>,
}

impl ReadStateManager {
    pub fn new(
        table: &MooncakeTable, // Assuming table.name() is available for logging
        replication_lsn_rx: watch::Receiver<u64>,
        table_commit_lsn_rx: watch::Receiver<u64>,
    ) -> Self {
        // new() is mostly setup.
        // table.get_state_for_reader() could be interesting if it's complex.
        // For now, profiling the whole new.
        let table_name_for_logs = "table_name".to_string(); // Assuming MooncakeTable has a name() method
        let _guard = ProfileGuard::new(&format!("RSM_new_for_table_{}", table_name_for_logs));

        let (table_snapshot_val, table_snapshot_watch_receiver_val) = {
            // Renamed to avoid conflict
            let _get_state_guard = ProfileGuard::new(&format!(
                "RSM_new_get_state_for_reader_table_{}",
                table_name_for_logs
            ));
            table.get_state_for_reader()
        };

        ReadStateManager {
            last_read_lsn: AtomicU64::new(0),
            last_read_state: RwLock::new(Arc::new(ReadState::new((vec![], vec![]), vec![]))),
            table_snapshot: table_snapshot_val,
            table_snapshot_watch_receiver: table_snapshot_watch_receiver_val,
            replication_lsn_rx,
            table_commit_lsn_rx,
        }
    }

    /// Attempts to read state at or after the specified LSN.
    /// If `lsn` is `None`, it attempts to read the latest available state.
    pub async fn try_read(&self, requested_lsn: Option<u64>) -> Result<Arc<ReadState>> {
        let _try_read_overall_guard =
            ProfileGuard::new(&format!("RSM_try_read_req_lsn_{:?}", requested_lsn));

        // 1. Early exit: If a specific LSN is requested and it's older than our last read,
        //    return the cached state.
        if let Some(req_lsn_val) = requested_lsn {
            if req_lsn_val < self.last_read_lsn.load(Ordering::Relaxed) {
                let _cache_hit_guard =
                    ProfileGuard::new(&format!("RSM_try_read_cache_hit_req_lsn_{}", req_lsn_val));
                let last_state_lock = {
                    // Renamed to avoid conflict
                    let _lock_guard =
                        ProfileGuard::new("RSM_try_read_cache_hit_last_read_state_lock_acquire");
                    self.last_read_state.read().await
                };
                return Ok(last_state_lock.clone());
            }
        }

        // Cloning receivers is cheap.
        let mut table_snapshot_rx_clone = self.table_snapshot_watch_receiver.clone(); // Renamed
        let mut replication_lsn_rx_clone = self.replication_lsn_rx.clone(); // Renamed
        let table_commit_lsn_rx_clone = self.table_commit_lsn_rx.clone(); // Renamed

        let mut loop_iteration = 0;
        loop {
            loop_iteration += 1;
            let _try_read_loop_iter_guard = ProfileGuard::new(&format!(
                "RSM_try_read_loop_iter_{}_req_lsn_{:?}",
                loop_iteration, requested_lsn
            ));

            // Borrowing from watch::Receiver is cheap.
            let current_snapshot_lsn = *table_snapshot_rx_clone.borrow();
            let current_replication_lsn = *replication_lsn_rx_clone.borrow();
            let current_commit_lsn = *table_commit_lsn_rx_clone.borrow();

            if self.can_satisfy_read_from_snapshot(
                // This is a synchronous check
                requested_lsn,
                current_snapshot_lsn,
                current_replication_lsn,
                current_commit_lsn,
            ) {
                // read_from_snapshot_and_update_cache is an async fn and profiled internally.
                return self
                    .read_from_snapshot_and_update_cache(
                        current_snapshot_lsn,
                        current_replication_lsn,
                        current_commit_lsn,
                    )
                    .await;
            }

            // If we reach here, we need to wait.
            // wait_for_relevant_lsn_change is an async fn and profiled internally.
            self.wait_for_relevant_lsn_change(
                requested_lsn.unwrap(), // unwrap is safe due to can_satisfy_read logic for None
                current_replication_lsn,
                &mut replication_lsn_rx_clone,
                &mut table_snapshot_rx_clone,
            )
            .await?;
        }
    }

    // This is a synchronous helper, its time is part of the caller's profiled block.
    fn can_satisfy_read_from_snapshot(
        &self,
        requested_lsn: Option<u64>,
        snapshot_lsn: u64,
        replication_lsn: u64,
        commit_lsn: u64,
    ) -> bool {
        // No ProfileGuard needed here as it's a quick, synchronous check.
        match requested_lsn {
            None => true,
            Some(req_lsn_val) => {
                req_lsn_val <= snapshot_lsn
                    || (req_lsn_val <= replication_lsn && snapshot_lsn == commit_lsn)
            }
        }
    }

    async fn read_from_snapshot_and_update_cache(
        &self,
        current_snapshot_lsn: u64,
        current_replication_lsn: u64,
        current_commit_lsn: u64,
    ) -> Result<Arc<ReadState>> {
        let _guard = ProfileGuard::new(&format!(
            "RSM_read_from_snapshot_update_cache_snaplsn_{}_replsn_{}_commlsn_{}",
            current_snapshot_lsn, current_replication_lsn, current_commit_lsn
        ));

        let table_state_snapshot_lock = {
            // Renamed
            let _lock_guard =
                ProfileGuard::new("RSM_read_from_snapshot_table_snapshot_lock_acquire");
            self.table_snapshot.read().await
        };
        let mut last_read_state_write_lock = {
            // Renamed
            let _lock_guard =
                ProfileGuard::new("RSM_read_from_snapshot_last_read_state_write_lock_acquire");
            self.last_read_state.write().await
        };

        // Atomic load/store are very fast.
        if self.last_read_lsn.load(Ordering::Acquire) < current_snapshot_lsn {
            let effective_lsn = if current_snapshot_lsn == current_commit_lsn
                && current_snapshot_lsn < current_replication_lsn
            {
                current_replication_lsn
            } else {
                current_snapshot_lsn
            };

            // This is the actual read from SnapshotTableState. This can involve I/O.
            let read_output_val = {
                // Renamed
                let _actual_read_guard = ProfileGuard::new(&format!(
                    "RSM_read_from_snapshot_SnapshotTableState_request_read_efflsn_{}",
                    effective_lsn
                ));
                table_state_snapshot_lock.request_read()?
            };

            self.last_read_lsn.store(effective_lsn, Ordering::Release);
            // ReadState::new is likely just struct instantiation.
            *last_read_state_write_lock = Arc::new(ReadState::new(
                (read_output_val.file_paths, read_output_val.deletions),
                read_output_val.associated_files,
            ));
        }
        Ok(last_read_state_write_lock.clone())
    }

    async fn wait_for_relevant_lsn_change(
        &self,
        requested_lsn_val: u64,
        current_replication_lsn: u64,
        replication_lsn_rx_param: &mut watch::Receiver<u64>, // Renamed parameter
        table_snapshot_rx_param: &mut watch::Receiver<u64>,  // Renamed parameter
    ) -> Result<()> {
        let _guard = ProfileGuard::new(&format!(
            "RSM_wait_for_lsn_change_req_{}_curr_repl_{}",
            requested_lsn_val, current_replication_lsn
        ));
        // The .changed().await is the actual blocking part.
        if requested_lsn_val > current_replication_lsn {
            let _wait_repl_guard =
                ProfileGuard::new("RSM_wait_for_lsn_change_await_replication_lsn");
            replication_lsn_rx_param
                .changed()
                .await
                .map_err(|e| Error::WatchChannelRecvError { source: e })?;
        } else {
            let _wait_snap_guard = ProfileGuard::new("RSM_wait_for_lsn_change_await_snapshot_lsn");
            table_snapshot_rx_param
                .changed()
                .await
                .map_err(|e| Error::WatchChannelRecvError { source: e })?;
        }
        Ok(())
    }
}
