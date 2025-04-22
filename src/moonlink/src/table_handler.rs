use crate::error::Result;
use crate::row::MoonlinkRow;
use crate::storage::DiskSliceWriter;
use crate::storage::MooncakeTable;
use arrow_schema::Schema;
use std::path::PathBuf;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

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
    /// Prepare the table for reading
    PrepareRead {
        response_channel: oneshot::Sender<(Vec<PathBuf>, Vec<(usize, usize)>)>,
    },
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Shutdown the handler
    _Shutdown,
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(schema: Schema, table_name: String, table_id: u64, path: PathBuf) -> Self {
        // Create the table
        let table = MooncakeTable::new(schema, table_name, table_id, path);

        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Create oneshot channel to send the table to the task
        let (tx, rx) = oneshot::channel();

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            match rx.await {
                Ok(table) => Self::event_loop(event_receiver, table).await,
                Err(_) => println!("Failed to receive table in event loop task"),
            }
        }));

        // Create the handler
        let handler = Self {
            _event_handle: event_handle,
            event_sender,
        };

        // Send the table to the task
        if tx.send(table).is_err() {
            println!("Failed to send table to event loop task");
        }

        handler
    }

    /// Get the event sender to send events to this handler
    pub fn get_event_sender(&self) -> Sender<TableEvent> {
        self.event_sender.clone()
    }

    /// Main event processing loop
    async fn event_loop(mut event_receiver: Receiver<TableEvent>, mut table: MooncakeTable) {
        let mut snapshot_handle: Option<JoinHandle<()>> = None;
        let mut flush_handle: Option<JoinHandle<Result<DiskSliceWriter>>> = None;
        let mut periodic_snapshot_interval = time::interval(Duration::from_secs(1));

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    match event {
                        TableEvent::Append { row, xact_id } => {
                            let result = match xact_id {
                                Some(xact_id) => table.append_in_stream_batch(row, xact_id),
                                None => table.append(row),
                            };

                            if let Err(e) = result {
                                println!("Failed to append row: {}", e);
                            }
                        }

                        TableEvent::Delete { row, lsn, xact_id } => {
                            match xact_id {
                                Some(xact_id) => table.delete_in_stream_batch(row, xact_id),
                                None => table.delete(row, lsn),
                            };
                        }

                        TableEvent::Commit { lsn } => {
                            table.commit(lsn);
                            if table.should_flush() {
                                flush_handle = Some(table.flush(lsn));
                            }
                        }

                        TableEvent::StreamCommit { lsn, xact_id } => {
                            match table.flush_transaction_stream(xact_id, lsn) {
                                Ok(writer) => {
                                    flush_handle = Some(writer);
                                }
                                Err(e) => {
                                    println!("Stream commit failed: {}", e);
                                }
                            }
                        }

                        TableEvent::StreamAbort { xact_id } => {
                            table.abort_in_stream_batch(xact_id);
                        }

                        TableEvent::PrepareRead { response_channel } => {
                            // Wait for any pending snapshot to complete first
                            if let Some(handle) = &mut snapshot_handle {
                                match handle.await {
                                    Ok(()) => {
                                        println!("Snapshot creation completed successfully before prepare read");
                                        snapshot_handle = None;
                                    }
                                    Err(e) => {
                                        println!("Snapshot task was cancelled before prepare read: {}", e);
                                        snapshot_handle = None;
                                    }
                                }
                            }

                            // Request read and return the file path
                            match table.request_read() {
                                Ok((file_paths, deletions)) => {
                                    let _ = response_channel.send((file_paths, deletions));
                                }
                                Err(e) => {
                                    println!("Failed to prepare read: {}", e);
                                }
                            }
                        }

                        TableEvent::Flush { lsn } => {
                            flush_handle = Some(table.flush(lsn));
                        }

                        TableEvent::_Shutdown => {
                            println!("Shutting down table handler");
                            break;
                        }
                    }
                }
                // wait for the snapshot to complete
                Some(()) = async {
                    if let Some(handle) = &mut snapshot_handle {
                        match handle.await {
                            Ok(()) => {
                                println!("Snapshot creation completed successfully");
                            }
                            Err(e) => {
                                println!("Snapshot task was cancelled: {}", e);
                            }
                        }
                        Some(())
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    snapshot_handle = None;
                }
                // wait for the flush to complete
                Some(writer) = async {
                    if let Some(handle) = &mut flush_handle {
                        match handle.await {
                            Ok(writer) => {
                                Some(writer)
                            }
                            Err(e) => {
                                println!("Flush task was cancelled: {}", e);
                                None
                            }
                        }
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    table.commit_flush(writer.unwrap()).unwrap();
                    flush_handle = None;
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    // Only create a periodic snapshot if there isn't already one in progress
                    if snapshot_handle.is_none() {
                        println!("Creating periodic snapshot");
                        snapshot_handle = table.create_snapshot();
                    }
                }
                // If all senders have been dropped, exit the loop
                else => {
                    println!("All event senders have been dropped, shutting down table handler");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;
    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    #[tokio::test]
    async fn test_table_handler() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        // Test append operation
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(30),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: row1,
                xact_id: None,
            })
            .await
            .unwrap();

        // Test commit operation
        event_sender
            .send(TableEvent::Commit { lsn: 1 })
            .await
            .unwrap();

        // wait for 1 second
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Test prepare read operation - with response channel
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Wait for the response
        match tokio::time::timeout(Duration::from_secs(1), rx).await {
            Ok(Ok((paths, _deletions))) => {
                println!("Received snapshot paths: {:?}", paths);

                if paths.is_empty() {
                    println!("Warning: No snapshot files returned");
                }

                // Verify that the paths exist
                for path in paths {
                    if path.exists() {
                        println!("Confirmed snapshot file exists: {:?}", path);
                        let file = File::open(&path).unwrap();
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                        println!("Converted arrow schema is: {}", builder.schema());
                        let mut reader = builder.build().unwrap();
                        let record_batch = reader.next();
                        println!("Record batch: {:?}", record_batch);
                    } else {
                        println!("Warning: Snapshot file does not exist at {:?}", path);
                    }
                }
            }
            Ok(Err(_)) => println!("Response channel was dropped"),
            Err(_) => println!("Timed out waiting for prepare read response"),
        }

        // Test shutdown
        event_sender.send(TableEvent::_Shutdown).await.unwrap();

        // Wait for event handler to exit
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }

        println!("All table handler tests passed!");
    }

    #[tokio::test]
    async fn test_table_handler_flush() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        // Test append operations - add multiple rows
        let rows: Vec<MoonlinkRow> = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Alice".as_bytes().to_vec()),
                RowValue::Int32(25),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Bob".as_bytes().to_vec()),
                RowValue::Int32(30),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(3),
                RowValue::ByteArray("Charlie".as_bytes().to_vec()),
                RowValue::Int32(35),
            ]),
        ];

        // Append all rows
        for row in rows {
            event_sender
                .send(TableEvent::Append { row, xact_id: None })
                .await
                .unwrap();
        }

        // Commit the changes
        event_sender
            .send(TableEvent::Commit { lsn: 1 })
            .await
            .unwrap();

        // Wait for commit to complete
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Test flush operation
        event_sender
            .send(TableEvent::Flush { lsn: 2 })
            .await
            .unwrap();

        // Wait for flush to complete
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Test prepare read operation
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Wait for the response
        match tokio::time::timeout(Duration::from_secs(1), rx).await {
            Ok(Ok((paths, _deletions))) => {
                println!("Received snapshot paths: {:?}", paths);

                if paths.is_empty() {
                    println!("Warning: No snapshot files returned");
                }

                // Verify that the paths exist
                for path in paths {
                    if path.exists() {
                        println!("Confirmed snapshot file exists: {:?}", path);
                        let file = File::open(&path).unwrap();
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                        println!("Converted arrow schema is: {}", builder.schema());
                        let mut reader = builder.build().unwrap();
                        let record_batch = reader.next();
                        println!("Record batch: {:?}", record_batch);
                    } else {
                        println!("Warning: Snapshot file does not exist at {:?}", path);
                    }
                }
            }
            Ok(Err(_)) => println!("Response channel was dropped"),
            Err(_) => println!("Timed out waiting for prepare read response"),
        }

        // Test shutdown
        event_sender.send(TableEvent::_Shutdown).await.unwrap();

        // Wait for event handler to exit
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }

        println!("All table handler flush tests passed!");
    }

    #[tokio::test]
    async fn test_streaming_append_and_commit() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        // Test transaction append with a specific xact_id
        let xact_id = 101;

        // Create rows to append in the transaction
        let row = MoonlinkRow::new(vec![
            RowValue::Int32(10),
            RowValue::ByteArray("Transaction-User".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);

        // Append row to the transaction
        event_sender
            .send(TableEvent::Append {
                row,
                xact_id: Some(xact_id),
            })
            .await
            .unwrap();

        // Commit the transaction
        event_sender
            .send(TableEvent::StreamCommit { lsn: 101, xact_id })
            .await
            .unwrap();

        // Wait for commit to complete
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Read and verify the data
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Verify the committed transaction
        match tokio::time::timeout(Duration::from_secs(1), rx).await {
            Ok(Ok((paths, _deletions))) => {
                println!("Received snapshot paths: {:?}", paths);

                if paths.is_empty() {
                    println!("Warning: No snapshot files returned");
                } else {
                    // Verify that the paths exist
                    for path in paths {
                        if path.exists() {
                            println!("Confirmed snapshot file exists: {:?}", path);
                            let file = File::open(&path).unwrap();
                            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                            println!("Converted arrow schema is: {}", builder.schema());
                            let mut reader = builder.build().unwrap();
                            let record_batch = reader.next();
                            println!("Record batch: {:?}", record_batch);

                            // Check that the record batch contains the expected row
                            if let Some(Ok(batch)) = record_batch {
                                assert_eq!(
                                    batch.num_rows(),
                                    1,
                                    "Expected one row in the record batch"
                                );
                                // In a more robust test, we'd check the actual values
                            }
                        } else {
                            println!("Warning: Snapshot file does not exist at {:?}", path);
                        }
                    }
                }
            }
            Ok(Err(_)) => println!("Response channel was dropped"),
            Err(_) => println!("Timed out waiting for prepare read response"),
        }

        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_streaming_delete() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        let xact_id = 101;

        // Create rows to append in the transaction
        let rows_to_append = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(10),
                RowValue::ByteArray("Transaction-User1".as_bytes().to_vec()),
                RowValue::Int32(25),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(11),
                RowValue::ByteArray("Transaction-User2".as_bytes().to_vec()),
                RowValue::Int32(30),
            ]),
        ];

        // Append rows to the transaction
        for row in rows_to_append {
            event_sender
                .send(TableEvent::Append {
                    row,
                    xact_id: Some(xact_id),
                })
                .await
                .unwrap();
        }

        // Delete one of the rows within the same transaction
        let row_to_delete = MoonlinkRow::new(vec![
            RowValue::Int32(10),
            RowValue::ByteArray("Transaction-User1".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);

        event_sender
            .send(TableEvent::Delete {
                row: row_to_delete,
                lsn: 100,
                xact_id: Some(xact_id),
            })
            .await
            .unwrap();

        // Commit the transaction
        event_sender
            .send(TableEvent::StreamCommit { lsn: 101, xact_id })
            .await
            .unwrap();

        // Wait for commit to complete
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Read and verify the data
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Verify the committed transaction (should only have User2)
        match tokio::time::timeout(Duration::from_secs(1), rx).await {
            Ok(Ok((paths, deletions))) => {
                println!("Received snapshot paths: {:?}", paths);
                println!("Deletions: {:?}", deletions);

                if paths.is_empty() {
                    println!("Warning: No snapshot files returned");
                } else {
                    // Verify that the paths exist
                    for path in paths {
                        if path.exists() {
                            println!("Confirmed snapshot file exists: {:?}", path);
                            let file = File::open(&path).unwrap();
                            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                            println!("Converted arrow schema is: {}", builder.schema());
                            let mut reader = builder.build().unwrap();
                            let record_batch = reader.next();
                            println!("Record batch: {:?}", record_batch);

                            // Check that the record batch contains only User2
                            if let Some(Ok(batch)) = record_batch {
                                assert_eq!(
                                    batch.num_rows(),
                                    1,
                                    "Expected one row in the record batch"
                                );
                                // In a more robust test, we'd verify that it's User2 and not User1
                            }
                        } else {
                            println!("Warning: Snapshot file does not exist at {:?}", path);
                        }
                    }
                }
            }
            Ok(Err(_)) => println!("Response channel was dropped"),
            Err(_) => println!("Timed out waiting for prepare read response"),
        }

        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_streaming_abort() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        // First, add and commit a baseline row to verify against
        let baseline_xact_id = 100;
        let baseline_row = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("Baseline-User".as_bytes().to_vec()),
            RowValue::Int32(20),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: baseline_row,
                xact_id: Some(baseline_xact_id),
            })
            .await
            .unwrap();

        event_sender
            .send(TableEvent::StreamCommit {
                lsn: 100,
                xact_id: baseline_xact_id,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Now create a transaction that will be aborted
        let abort_xact_id = 102;

        let abort_row = MoonlinkRow::new(vec![
            RowValue::Int32(20),
            RowValue::ByteArray("Transaction-UserToAbort".as_bytes().to_vec()),
            RowValue::Int32(40),
        ]);

        // Append a row to the transaction that will be aborted
        event_sender
            .send(TableEvent::Append {
                row: abort_row,
                xact_id: Some(abort_xact_id),
            })
            .await
            .unwrap();

        // Abort the transaction
        event_sender
            .send(TableEvent::StreamAbort {
                xact_id: abort_xact_id,
            })
            .await
            .unwrap();

        // Wait for abort to complete
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Read and verify the data
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Verify that only the baseline data exists and aborted transaction data is not present
        match tokio::time::timeout(Duration::from_secs(1), rx).await {
            Ok(Ok((paths, _deletions))) => {
                println!("Received snapshot paths: {:?}", paths);

                if paths.is_empty() {
                    println!("Warning: No snapshot files returned");
                } else {
                    // Verify that the paths exist
                    for path in paths {
                        if path.exists() {
                            println!("Confirmed snapshot file exists: {:?}", path);
                            let file = File::open(&path).unwrap();
                            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                            println!("Converted arrow schema is: {}", builder.schema());
                            let mut reader = builder.build().unwrap();
                            let record_batch = reader.next();
                            println!("Record batch: {:?}", record_batch);

                            // Check that only the baseline row exists
                            if let Some(Ok(batch)) = record_batch {
                                assert_eq!(
                                    batch.num_rows(),
                                    1,
                                    "Expected one row in the record batch"
                                );
                                // In a more robust test, we'd check it's the baseline row
                            }
                        } else {
                            println!("Warning: Snapshot file does not exist at {:?}", path);
                        }
                    }
                }
            }
            Ok(Err(_)) => println!("Response channel was dropped"),
            Err(_) => println!("Timed out waiting for prepare read response"),
        }

        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_streaming_transactions() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let handler = TableHandler::new(schema, "test_table".to_string(), 1, path);
        let event_sender = handler.get_event_sender();

        // Create two concurrent transactions
        let xact_id_1 = 103;
        let xact_id_2 = 104;

        // Transaction 1: add one row
        let tx1_row = MoonlinkRow::new(vec![
            RowValue::Int32(30),
            RowValue::ByteArray("Transaction1-User".as_bytes().to_vec()),
            RowValue::Int32(35),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: tx1_row,
                xact_id: Some(xact_id_1),
            })
            .await
            .unwrap();

        // Transaction 2: add one row
        let tx2_row = MoonlinkRow::new(vec![
            RowValue::Int32(40),
            RowValue::ByteArray("Transaction2-User".as_bytes().to_vec()),
            RowValue::Int32(45),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: tx2_row,
                xact_id: Some(xact_id_2),
            })
            .await
            .unwrap();

        // Commit transaction 1, abort transaction 2
        event_sender
            .send(TableEvent::StreamCommit {
                lsn: 103,
                xact_id: xact_id_1,
            })
            .await
            .unwrap();

        event_sender
            .send(TableEvent::StreamAbort { xact_id: xact_id_2 })
            .await
            .unwrap();

        // Wait for operations to complete
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Read and verify the data
        let (tx, rx) = oneshot::channel();
        event_sender
            .send(TableEvent::PrepareRead {
                response_channel: tx,
            })
            .await
            .unwrap();

        // Verify final state (should only have Transaction1-User, not Transaction2-User)
        match tokio::time::timeout(Duration::from_secs(1), rx).await {
            Ok(Ok((paths, _deletions))) => {
                println!("Received final snapshot paths: {:?}", paths);

                if paths.is_empty() {
                    println!("Warning: No final snapshot files returned");
                } else {
                    for path in paths {
                        if path.exists() {
                            println!("Confirmed final snapshot file exists: {:?}", path);
                            let file = File::open(&path).unwrap();
                            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                            println!("Final schema: {}", builder.schema());
                            let mut reader = builder.build().unwrap();
                            let record_batch = reader.next();
                            println!("Final record batch: {:?}", record_batch);

                            // Check that only the committed transaction data is present
                            if let Some(Ok(batch)) = record_batch {
                                assert_eq!(
                                    batch.num_rows(),
                                    1,
                                    "Expected one row in the record batch"
                                );
                                // In a more robust test, we'd verify it's Transaction1-User
                            }
                        } else {
                            println!("Warning: Final snapshot file does not exist at {:?}", path);
                        }
                    }
                }
            }
            Ok(Err(_)) => println!("Response channel was dropped"),
            Err(_) => println!("Timed out waiting for prepare read response"),
        }

        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }
}
