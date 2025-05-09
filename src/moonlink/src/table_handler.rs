use crate::row::MoonlinkRow;
use crate::storage::MooncakeTable; // MooncakeTable methods are key
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

// Add ProfileGuard
use crate::profiling::ProfileGuard;

/// Event types that can be processed by the TableHandler
#[derive(Debug)]
pub enum TableEvent {
    /// Append a row to the table
    Append {
        row: MoonlinkRow,
        xact_id: Option<u32>,
    },
    /// Delete a row from the table
    Delete {
        row: MoonlinkRow,
        lsn: u64,
        xact_id: Option<u32>,
    },
    /// Commit all pending operations with a given LSN
    Commit { lsn: u64 },
    /// Commit all pending operations with given LSN and xact_id
    StreamCommit { lsn: u64, xact_id: u32 },
    /// Abort current stream with given xact_id
    StreamAbort { xact_id: u32 },
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Flush the transaction stream with given xact_id
    StreamFlush { xact_id: u32 },
    /// Shutdown the handler
    _Shutdown,
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>, // Not profiling the JoinHandle itself directly

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(table: MooncakeTable) -> Self {
        // new() itself is mostly setup. The main work is in the spawned event_loop.
        // We can't directly profile the whole event_loop with one guard from here easily
        // because it's spawned. Profiling will happen *inside* the event_loop.
        // Table name can be useful for distinguishing logs if multiple TableHandlers exist.
        let table_name_for_logs = "table_name".to_string(); // Assuming MooncakeTable has a name() method
        let _guard = ProfileGuard::new(&format!(
            "TABLE_HANDLER_new_for_table_{}",
            table_name_for_logs
        ));

        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100); // Channel creation is fast

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            // Pass table_name_for_logs into the event_loop for its profiling tags
            Self::event_loop(event_receiver, table, table_name_for_logs).await;
        }));

        // Create the handler
        Self {
            _event_handle: event_handle,
            event_sender,
        }
    }

    /// Get the event sender to send events to this handler
    pub fn get_event_sender(&self) -> Sender<TableEvent> {
        // Cloning an mpsc sender is cheap.
        self.event_sender.clone()
    }

    /// Main event processing loop
    async fn event_loop(
        mut event_receiver: Receiver<TableEvent>,
        mut table: MooncakeTable,
        table_name_for_logs: String, // Receive table name for log tags
    ) {
        let _event_loop_overall_guard = ProfileGuard::new(&format!(
            "TABLE_HANDLER_event_loop_start_table_{}",
            table_name_for_logs
        ));
        let mut snapshot_handle: Option<JoinHandle<u64>> = None;
        let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            // The select! macro itself isn't profiled, but the branches are.
            // The time ProfileGuard reports for a branch will be the time spent *in that branch's execution*.
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    let event_type_str = format!("{:?}", event); // For tag, might be verbose

                    match event {
                        TableEvent::Append { row, xact_id } => {
                            let result = match xact_id {
                                Some(xid_val) => { // Renamed xact_id to xid_val
                                    // table.append_in_stream_batch is synchronous
                                    let res = table.append_in_stream_batch(row, xid_val);
                                    if table.should_transaction_flush(xid_val) {
                                        println!("Flushing transaction stream"); // Existing log
                                        let _flush_tx_guard = ProfileGuard::new(&format!(
                                            "TABLE_HANDLER_Append_flush_transaction_stream_table_{}_xid_{}",
                                            table_name_for_logs, xid_val
                                        ));
                                        if let Err(e) = table.flush_transaction_stream(xid_val).await {
                                            println!("Flush failed in Append: {}", e); // Existing log
                                        }
                                    }
                                    res
                                },
                                None => table.append(row), // table.append is synchronous
                            };

                            if let Err(e) = result {
                                println!("Failed to append row: {}", e); // Existing log
                            }
                        }
                        TableEvent::Delete { row, lsn, xact_id } => {
                            match xact_id {
                                Some(xid_val) => table.delete_in_stream_batch(row, xid_val), // sync
                                None => table.delete(row, lsn), // sync
                            };
                        }
                        TableEvent::Commit { lsn } => {
                            let _commit_guard = ProfileGuard::new(&format!(
                                "TABLE_HANDLER_Commit_table_{}_lsn_{}",
                                table_name_for_logs, lsn
                            ));
                            table.commit(lsn); // sync
                            if table.should_flush() { // sync check
                                let _flush_guard = ProfileGuard::new(&format!(
                                    "TABLE_HANDLER_Commit_flush_table_{}_lsn_{}",
                                    table_name_for_logs, lsn
                                ));
                                if let Err(e) = table.flush(lsn).await { // async flush
                                    println!("Flush failed in Commit: {}", e); // Existing log
                                }
                            }
                        }
                        TableEvent::StreamCommit { lsn, xact_id } => {
                            let _stream_commit_guard = ProfileGuard::new(&format!(
                                "TABLE_HANDLER_StreamCommit_table_{}_lsn_{}_xid_{}",
                                table_name_for_logs, lsn, xact_id
                            ));
                            // This is an async flush operation.
                            if let Err(e) = table.commit_transaction_stream(xact_id, lsn).await {
                                println!("Stream commit flush failed: {}", e); // Existing log
                            }
                        }
                        TableEvent::StreamAbort { xact_id } => {
                            let _stream_abort_guard = ProfileGuard::new(&format!(
                                "TABLE_HANDLER_StreamAbort_table_{}_xid_{}",
                                table_name_for_logs, xact_id
                            ));
                            table.abort_in_stream_batch(xact_id); // sync
                        }
                        TableEvent::Flush { lsn } => {
                            let _flush_event_guard = ProfileGuard::new(&format!(
                                "TABLE_HANDLER_FlushEvent_table_{}_lsn_{}",
                                table_name_for_logs, lsn
                            ));
                            // This is an async flush operation.
                            if let Err(e) = table.flush(lsn).await {
                                println!("Explicit Flush failed: {}", e); // Existing log
                            }
                        }
                        TableEvent::StreamFlush { xact_id } => {
                             let _stream_flush_event_guard = ProfileGuard::new(&format!(
                                "TABLE_HANDLER_StreamFlushEvent_table_{}_xid_{}",
                                table_name_for_logs, xact_id
                            ));
                            // This is an async flush operation.
                            if let Err(e) = table.flush_transaction_stream(xact_id).await {
                                println!("Stream flush failed: {}", e); // Existing log
                            }
                        }
                        TableEvent::_Shutdown => {
                            println!("Shutting down table handler"); // Existing log
                            break;
                        }
                    }
                }
                // wait for the snapshot to complete
                Some(()) = async {
                    if let Some(handle) = &mut snapshot_handle {
                        match handle.await { // This await is the actual work of waiting for snapshot
                            Ok(lsn) => {
                                table.notify_snapshot_reader(lsn); // sync
                            }
                            Err(e) => {
                                println!("Snapshot task was cancelled: {}", e); // Existing log
                            }
                        }
                        Some(())
                    } else {
                        futures::future::pending::<Option<_>>().await // This makes the branch non-immediately ready
                    }
                }, if snapshot_handle.is_some() => { // Conditionally enable this branch
                    let _snapshot_completion_guard = ProfileGuard::new(&format!(
                        "TABLE_HANDLER_event_loop_snapshot_completion_table_{}",
                        table_name_for_logs
                    ));
                    // The actual work (handle.await) is inside the async block above.
                    // This guard measures the processing after it's known to be complete.
                    snapshot_handle = None;
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    let _periodic_tick_guard = ProfileGuard::new(&format!(
                        "TABLE_HANDLER_event_loop_periodic_snapshot_tick_table_{}",
                        table_name_for_logs
                    ));
                    // Only create a periodic snapshot if there isn't already one in progress
                    if snapshot_handle.is_none() {
                        // table.create_snapshot() likely returns Option<JoinHandle> and starts an async task.
                        // The time here is for initiating the snapshot.
                        let _create_snapshot_guard = ProfileGuard::new(&format!(
                            "TABLE_HANDLER_event_loop_create_snapshot_table_{}",
                            table_name_for_logs
                        ));
                        snapshot_handle = table.create_snapshot();
                    }
                }
                // If all senders have been dropped, exit the loop
                else => {
                    println!("All event senders have been dropped, shutting down table handler"); // Existing log
                    break;
                }
            }
        }
        let _event_loop_shutdown_guard = ProfileGuard::new(&format!(
            "TABLE_HANDLER_event_loop_shutdown_table_{}",
            table_name_for_logs
        ));
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_utils;
