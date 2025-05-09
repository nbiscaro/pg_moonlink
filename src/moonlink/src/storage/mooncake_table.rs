mod data_batches;
pub(crate) mod delete_vector;
mod disk_slice;
mod mem_slice;
mod shared_array;
mod snapshot;

use super::iceberg::iceberg_table_manager::IcebergTableManagerConfig;
use super::index::{MemIndex, MooncakeIndex};
use super::storage_utils::{RawDeletionRecord, RecordLocation};
use crate::error::{Error, Result};
use crate::row::{Identity, MoonlinkRow};
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use std::collections::HashMap;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter; // Used in flushing
use mem_slice::MemSlice; // MemSlice operations are mostly synchronous and fast
pub(crate) use snapshot::SnapshotTableState; // SnapshotTableState.update_snapshot is key
use tokio::spawn;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;

// Add ProfileGuard
use crate::profiling::ProfileGuard;

#[derive(Debug)]
pub(crate) struct TableConfig {
    _mem_slice_size: usize,
    batch_size: usize,
}

impl TableConfig {
    #[cfg(debug_assertions)]
    const DEFAULT_MEM_SLICE_SIZE: usize = 2048 * 2;
    #[cfg(debug_assertions)]
    const DEFAULT_BATCH_SIZE: usize = 4;

    #[cfg(not(debug_assertions))]
    const DEFAULT_MEM_SLICE_SIZE: usize = 2048 * 16;
    #[cfg(not(debug_assertions))]
    const DEFAULT_BATCH_SIZE: usize = 2048;

    pub fn new() -> Self {
        // new() is cheap
        Self {
            _mem_slice_size: Self::DEFAULT_MEM_SLICE_SIZE,
            batch_size: Self::DEFAULT_BATCH_SIZE,
        }
    }
}

#[derive(Debug)]
pub struct TableMetadata {
    pub(crate) name: String,
    pub(crate) id: u64,
    pub(crate) schema: Arc<Schema>,
    pub(crate) config: TableConfig,
    pub(crate) path: PathBuf,
    pub(crate) identity: Identity,
}

pub struct Snapshot {
    pub(crate) metadata: Arc<TableMetadata>,
    pub(crate) disk_files: HashMap<PathBuf, BatchDeletionVector>,
    snapshot_version: u64,
    indices: MooncakeIndex,
}

impl Snapshot {
    pub(crate) fn new(metadata: Arc<TableMetadata>) -> Self {
        // new() is cheap
        Self {
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            indices: MooncakeIndex::new(),
        }
    }

    pub fn get_name_for_inmemory_file(&self) -> PathBuf {
        // cheap
        Path::join(
            &self.metadata.path,
            format!(
                "inmemory_{}_{}_{}.parquet",
                self.metadata.name, self.metadata.id, self.snapshot_version
            ),
        )
    }
}

#[derive(Default)]
pub struct SnapshotTask {
    new_disk_slices: Vec<DiskSliceWriter>,
    new_deletions: Vec<RawDeletionRecord>,
    new_record_batches: Vec<(u64, Arc<RecordBatch>)>,
    new_rows: Option<SharedRowBufferSnapshot>,
    new_mem_indices: Vec<Arc<MemIndex>>,
    new_lsn: u64,
    new_commit_point: Option<RecordLocation>,
}

impl SnapshotTask {
    pub fn new() -> Self {
        // new() is cheap
        Self {
            new_disk_slices: Vec::new(),
            new_deletions: Vec::new(),
            new_record_batches: Vec::new(),
            new_rows: None,
            new_mem_indices: Vec::new(),
            new_lsn: 0,
            new_commit_point: None,
        }
    }

    pub fn should_create_snapshot(&self) -> bool {
        // cheap
        self.new_lsn > 0 || !self.new_disk_slices.is_empty() || self.new_deletions.len() > 1000
    }
}

struct TransactionStreamState {
    mem_slice: MemSlice,
    new_deletions: Vec<RawDeletionRecord>,
    new_disk_slices: Vec<DiskSliceWriter>,
}

impl TransactionStreamState {
    fn new(schema: Arc<Schema>, batch_size: usize) -> Self {
        // new() is cheap, MemSlice::new is also cheap
        Self {
            mem_slice: MemSlice::new(schema, batch_size),
            new_deletions: Vec::new(),
            new_disk_slices: Vec::new(),
        }
    }
}

pub struct MooncakeTable {
    metadata: Arc<TableMetadata>,
    mem_slice: MemSlice,
    snapshot: Arc<RwLock<SnapshotTableState>>,
    table_snapshot_watch_sender: watch::Sender<u64>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    next_snapshot_task: SnapshotTask,
    transaction_stream_states: HashMap<u32, TransactionStreamState>,
}

impl MooncakeTable {
    pub fn new(
        schema: Schema,
        name: String,
        version: u64,
        base_path: PathBuf,
        identity: Identity,
        iceberg_table_config: Option<IcebergTableManagerConfig>,
    ) -> Self {
        // new() is mostly cheap setup of structs.
        // SnapshotTableState::new might do some work if iceberg_table_config is Some and complex.
        let _guard = ProfileGuard::new(&format!("MOONCAKE_TABLE_new_name_{}_id_{}", name, version));
        let table_config = TableConfig::new();
        let schema_arc = Arc::new(schema); // Renamed from schema to schema_arc
        let metadata_arc = Arc::new(TableMetadata {
            // Renamed from metadata
            name,
            id: version,
            schema: schema_arc.clone(), // Use schema_arc
            config: table_config,
            path: base_path,
            identity,
        });
        let (table_snapshot_watch_sender_val, table_snapshot_watch_receiver_val) =
            watch::channel(0); // Renamed

        let snapshot_val = {
            // Renamed
            let _sts_new_guard = ProfileGuard::new(&format!(
                "MOONCAKE_TABLE_new_SnapshotTableState_new_name_{}",
                metadata_arc.name
            ));
            Arc::new(RwLock::new(SnapshotTableState::new(
                metadata_arc.clone(), // Use metadata_arc
                iceberg_table_config,
            )))
        };

        Self {
            mem_slice: MemSlice::new(metadata_arc.schema.clone(), metadata_arc.config.batch_size), // Use metadata_arc
            metadata: metadata_arc.clone(), // Use metadata_arc
            snapshot: snapshot_val,
            next_snapshot_task: SnapshotTask::new(),
            transaction_stream_states: HashMap::new(),
            table_snapshot_watch_sender: table_snapshot_watch_sender_val,
            table_snapshot_watch_receiver: table_snapshot_watch_receiver_val,
        }
    }

    pub(crate) fn get_state_for_reader(
        &self,
    ) -> (Arc<RwLock<SnapshotTableState>>, watch::Receiver<u64>) {
        // This is a cheap clone operation.
        (
            self.snapshot.clone(),
            self.table_snapshot_watch_receiver.clone(),
        )
    }

    // Expose name for TableHandler logging
    pub(crate) fn name(&self) -> &str {
        &self.metadata.name
    }

    pub fn append(&mut self, row: MoonlinkRow) -> Result<()> {
        // This is a synchronous, in-memory operation. Usually fast.
        // Profiling it if MemSlice::append becomes complex or a bottleneck.
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_append_name_{}",
            self.metadata.name
        ));
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        if let Some(batch) = self.mem_slice.append(lookup_key, row)? {
            self.next_snapshot_task.new_record_batches.push(batch);
        }
        Ok(())
    }

    pub fn delete(&mut self, row: MoonlinkRow, lsn: u64) {
        // Synchronous, in-memory.
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_delete_name_{}_lsn_{}",
            self.metadata.name, lsn
        ));
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn,
            pos: None,
            row_identity: self.metadata.identity.extract_identity_columns(row),
        };
        let pos = self.mem_slice.delete(&record, &self.metadata.identity);
        record.pos = pos;
        self.next_snapshot_task.new_deletions.push(record);
    }

    pub fn commit(&mut self, lsn: u64) {
        // Synchronous, very fast state update.
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_commit_name_{}_lsn_{}",
            self.metadata.name, lsn
        ));
        self.next_snapshot_task.new_lsn = lsn;
        self.next_snapshot_task.new_commit_point = Some(self.mem_slice.get_commit_check_point());
    }

    pub fn should_flush(&self) -> bool {
        // Synchronous, fast check.
        self.mem_slice.get_num_rows() >= self.metadata.config.batch_size
    }

    fn get_or_create_stream_state<'a>(
        // Static method, not profiling directly, called by others.
        transaction_stream_states: &'a mut HashMap<u32, TransactionStreamState>,
        metadata: &Arc<TableMetadata>,
        xact_id: u32,
    ) -> &'a mut TransactionStreamState {
        transaction_stream_states.entry(xact_id).or_insert_with(|| {
            TransactionStreamState::new(metadata.schema.clone(), metadata.config.batch_size)
        })
    }

    pub fn should_transaction_flush(&self, xact_id: u32) -> bool {
        // Synchronous, fast check.
        self.transaction_stream_states
            .get(&xact_id)
            .unwrap()
            .mem_slice
            .get_num_rows()
            >= self.metadata.config.batch_size
    }

    pub fn append_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) -> Result<()> {
        // Synchronous, in-memory.
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let stream_state_ref = Self::get_or_create_stream_state(
            // Renamed
            &mut self.transaction_stream_states,
            &self.metadata,
            xact_id,
        );

        stream_state_ref.mem_slice.append(lookup_key, row)?;

        Ok(())
    }

    pub fn delete_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) {
        // Synchronous, in-memory, though with a bit more logic.
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn: u64::MAX,
            pos: None,
            row_identity: self.metadata.identity.extract_identity_columns(row),
        };

        let stream_state_ref = Self::get_or_create_stream_state(
            // Renamed
            &mut self.transaction_stream_states,
            &self.metadata,
            xact_id,
        );
        if let Some(pos) = stream_state_ref
            .mem_slice
            .delete(&record, &self.metadata.identity)
        {
            record.pos = Some(pos);
        } else {
            record.pos = self
                .mem_slice
                .find_non_deleted_position(&record, &self.metadata.identity);
        }
        self.transaction_stream_states
            .get_mut(&xact_id)
            .unwrap()
            .new_deletions
            .push(record);
    }

    pub fn abort_in_stream_batch(&mut self, xact_id: u32) {
        // Synchronous, fast.
        self.transaction_stream_states.remove(&xact_id);
    }

    async fn inner_flush_data_files(
        // Helper, called by public flush methods
        mem_slice: &mut MemSlice,
        snapshot_task: &mut SnapshotTask,
        metadata: &Arc<TableMetadata>,
        lsn: Option<u64>,
    ) -> Result<DiskSliceWriter> {
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_inner_flush_data_files_name_{}_lsn_{:?}",
            metadata.name, lsn
        ));
        let (new_batch, batches_val, index_val) = mem_slice.drain().unwrap(); // Renamed, drain is sync

        if let Some(batch) = new_batch {
            snapshot_task.new_record_batches.push(batch);
        }

        let index_arc = Arc::new(index_val); // Renamed
        snapshot_task.new_mem_indices.push(index_arc.clone());

        let metadata_clone = metadata.clone();
        let path_clone = metadata.path.clone();

        // This is the core I/O part: writing to disk in a blocking task.
        let disk_slice_writer_join_handle_val = {
            // Renamed
            let _spawn_blocking_guard = ProfileGuard::new(&format!(
                "MOONCAKE_TABLE_inner_flush_spawn_blocking_disk_write_name_{}",
                metadata.name
            ));
            tokio::task::spawn_blocking(move || {
                // The work inside spawn_blocking is not directly profiled by this outer guard's Drop,
                // but the await on the handle will include its time.
                // For very detailed disk write timing, DiskSliceWriter::write would need profiling.
                let mut disk_slice = DiskSliceWriter::new(
                    metadata_clone.schema.clone(),
                    path_clone,
                    batches_val, // Use renamed
                    lsn,
                    index_arc, // Use renamed
                );
                disk_slice.write()?;
                Ok(disk_slice)
            })
        };

        match disk_slice_writer_join_handle_val.await {
            Ok(Ok(disk_slice)) => Ok(disk_slice),
            Ok(Err(e)) => Err(e),
            Err(join_error) => Err(Error::TokioJoinError(join_error.to_string())),
        }
    }

    async fn stream_flush(
        // Helper, called by public flush methods
        mem_slice: &mut MemSlice,
        metadata: &Arc<TableMetadata>,
    ) -> Result<DiskSliceWriter> {
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_stream_flush_name_{}",
            metadata.name
        ));
        let (_, batches_val, index_val) = mem_slice.drain().unwrap(); // Renamed, drain is sync

        let index_arc = Arc::new(index_val); // Renamed

        let schema_clone = metadata.schema.clone();
        let path_clone = metadata.path.clone();

        // Core I/O part.
        let disk_slice_join_handle_val = {
            // Renamed
            let _spawn_blocking_guard = ProfileGuard::new(&format!(
                "MOONCAKE_TABLE_stream_flush_spawn_blocking_disk_write_name_{}",
                metadata.name
            ));
            tokio::task::spawn_blocking(move || {
                let mut disk_slice =
                    DiskSliceWriter::new(schema_clone, path_clone, batches_val, None, index_arc); // Use renamed
                disk_slice.write()?;
                Ok(disk_slice)
            })
        };

        match disk_slice_join_handle_val.await {
            Ok(Ok(disk_slice)) => Ok(disk_slice),
            Ok(Err(e)) => Err(e),
            Err(join_error) => Err(Error::TokioJoinError(join_error.to_string())),
        }
    }

    pub async fn flush_transaction_stream(&mut self, xact_id: u32) -> Result<()> {
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_flush_transaction_stream_name_{}_xid_{}",
            self.metadata.name, xact_id
        ));
        if let Some(stream_state_ref) = self.transaction_stream_states.get_mut(&xact_id) {
            // Renamed
            // Self::stream_flush is profiled internally.
            let disk_slice_val = // Renamed
                Self::stream_flush(&mut stream_state_ref.mem_slice, &self.metadata).await?;
            stream_state_ref.new_disk_slices.push(disk_slice_val);
            return Ok(());
        }
        Ok(())
    }

    pub async fn commit_transaction_stream(&mut self, xact_id: u32, lsn: u64) -> Result<()> {
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_commit_transaction_stream_name_{}_xid_{}_lsn_{}",
            self.metadata.name, xact_id, lsn
        ));
        if let Some(mut stream_state_val) = self.transaction_stream_states.remove(&xact_id) {
            // Renamed
            let xact_mem_slice_ref = &mut stream_state_val.mem_slice; // Renamed

            let snapshot_task_ref = &mut self.next_snapshot_task; // Renamed
            snapshot_task_ref.new_lsn = lsn;

            for deletion in stream_state_val.new_deletions.iter_mut() {
                deletion.lsn = lsn;
            }
            snapshot_task_ref
                .new_deletions
                .append(&mut stream_state_val.new_deletions);

            // Self::stream_flush is profiled internally.
            let disk_slice_val = Self::stream_flush(xact_mem_slice_ref, &self.metadata).await?; // Renamed
            stream_state_val.new_disk_slices.push(disk_slice_val);

            for disk_slice_ref in stream_state_val.new_disk_slices.iter_mut() {
                // Renamed
                disk_slice_ref.set_lsn(Some(lsn));
            }
            snapshot_task_ref
                .new_disk_slices
                .append(&mut stream_state_val.new_disk_slices);
            Ok(())
        } else {
            Err(Error::TransactionNotFound(xact_id))
        }
    }

    pub async fn flush(&mut self, lsn: u64) -> Result<()> {
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_flush_name_{}_lsn_{}",
            self.metadata.name, lsn
        ));
        // Self::inner_flush_data_files is profiled internally.
        let disk_slice_val = Self::inner_flush_data_files(
            // Renamed
            &mut self.mem_slice,
            &mut self.next_snapshot_task,
            &self.metadata,
            Some(lsn),
        )
        .await?;
        self.next_snapshot_task.new_disk_slices.push(disk_slice_val);
        Ok(())
    }

    pub fn create_snapshot(&mut self) -> Option<JoinHandle<u64>> {
        // This method itself is synchronous and mostly conditional logic.
        // The actual work is in the spawned Self::create_snapshot_async.
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_create_snapshot_name_{}",
            self.metadata.name
        ));
        if !self.next_snapshot_task.should_create_snapshot() {
            return None;
        }
        self.next_snapshot_task.new_rows = Some(self.mem_slice.get_latest_rows());
        let next_snapshot_task_val = take(&mut self.next_snapshot_task); // Renamed

        // The spawn itself is fast. The JoinHandle will complete when create_snapshot_async finishes.
        // The _spawn_guard measures the time to spawn this task.
        let _spawn_guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_create_snapshot_spawn_async_name_{}",
            self.metadata.name
        ));
        Some(spawn(Self::create_snapshot_async(
            // create_snapshot_async is profiled internally
            self.snapshot.clone(),
            next_snapshot_task_val, // Use renamed
        )))
    }

    pub(crate) fn notify_snapshot_reader(&self, lsn: u64) {
        // watch::Sender::send is usually very fast (non-blocking).
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_TABLE_notify_snapshot_reader_name_{}_lsn_{}",
            self.metadata.name, lsn
        ));
        self.table_snapshot_watch_sender.send(lsn).unwrap();
    }

    async fn create_snapshot_async(
        // Helper, called by public create_snapshot
        snapshot_param: Arc<RwLock<SnapshotTableState>>, // Renamed
        next_snapshot_task_param: SnapshotTask,          // Renamed
    ) -> u64 {
        // This is a critical async function where the snapshot is actually updated.
        let _guard = ProfileGuard::new("MOONCAKE_TABLE_create_snapshot_async");
        let mut snapshot_write_lock = {
            // Renamed
            let _lock_guard =
                ProfileGuard::new("MOONCAKE_TABLE_create_snapshot_async_snapshot_lock_acquire");
            snapshot_param.write().await
        };
        // snapshot_write_lock.update_snapshot() is the core work here.
        // This should be profiled if it's an async function itself, or its constituent parts.
        // For now, profiling the call to it.
        {
            let _update_guard =
                ProfileGuard::new("MOONCAKE_TABLE_create_snapshot_async_update_snapshot_call");
            snapshot_write_lock
                .update_snapshot(next_snapshot_task_param) // Use renamed
                .await
        }
        // The return value is u64, not the updated snapshot itself, so no further profiling on return.
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;
