use super::index::index_util::{get_lookup_key, Index, MemIndex, MooncakeIndex};
use crate::error::{Error, Result};
use crate::row::MoonlinkRow;
use crate::storage::data_batches::InMemoryBatch;
use crate::storage::delete_vector::BatchDeletionVector;
use crate::storage::disk_slice::DiskSliceWriter;
use crate::storage::mem_slice::MemSlice;
use crate::storage::table_utils::{ProcessedDeletionRecord, RawDeletionRecord, RecordLocation};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::collections::{BTreeMap, HashMap};
use std::mem::{swap, take};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::spawn;
use tokio::task::JoinHandle;
struct TableConfig {
    /// mem slice size
    ///
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
        Self {
            _mem_slice_size: Self::DEFAULT_MEM_SLICE_SIZE,
            batch_size: Self::DEFAULT_BATCH_SIZE,
        }
    }
}

struct TableMetadata {
    /// table name
    name: String,
    /// table id
    id: u64,
    /// table schema
    schema: Arc<Schema>,
    /// table config
    config: TableConfig,
    /// storage path
    path: PathBuf,
    /// function to get lookup key from row
    pub(crate) get_lookup_key: fn(&MoonlinkRow) -> i64,
}

/// Snapshot contains state of the table at a given time.
/// A snapshot maps directly to an iceberg snapshot.
///
pub struct Snapshot {
    /// table metadata
    metadata: Arc<TableMetadata>,
    /// datafile and their deletion vectors
    disk_files: HashMap<PathBuf, BatchDeletionVector>,
    /// Current snapshot version
    snapshot_version: u64,
    /// indices
    indices: MooncakeIndex,
}

impl Snapshot {
    fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            indices: MooncakeIndex::new(),
        }
    }

    pub fn get_name_for_inmemory_file(&self) -> PathBuf {
        Path::join(
            &self.metadata.path,
            format!(
                "inmemory_{}_{}_{}.parquet",
                self.metadata.name, self.metadata.id, self.snapshot_version
            ),
        )
    }
}

pub struct SnapshotTask {
    /// Current task
    ///
    new_disk_slices: Vec<DiskSliceWriter>,
    new_deletions: Vec<RawDeletionRecord>,
    new_record_batches: Vec<(u64, Arc<RecordBatch>)>,
    new_rows: Vec<MoonlinkRow>,
    new_mem_indices: Vec<Arc<MemIndex>>,
    new_lsn: u64,
    new_commit_point: Option<RecordLocation>,
}

impl SnapshotTask {
    pub fn new() -> Self {
        Self {
            new_disk_slices: Vec::new(),
            new_deletions: Vec::new(),
            new_record_batches: Vec::new(),
            new_rows: Vec::new(),
            new_mem_indices: Vec::new(),
            new_lsn: 0,
            new_commit_point: None,
        }
    }

    pub fn should_create_snapshot(&self) -> bool {
        self.new_lsn > 0 || self.new_disk_slices.len() > 0
    }

    pub fn take_task(&mut self, other: &mut Self) {
        self.new_lsn = other.new_lsn;
        other.new_lsn = 0;
        self.new_commit_point = take(&mut other.new_commit_point);
        self.new_disk_slices = take(&mut other.new_disk_slices);
        self.new_deletions = take(&mut other.new_deletions);
        self.new_record_batches = take(&mut other.new_record_batches);
        self.new_rows = take(&mut other.new_rows);
        self.new_mem_indices = take(&mut other.new_mem_indices);
    }
}
pub struct SnapshotTableState {
    /// Current snapshot
    current_snapshot: Snapshot,

    /// In memory RecordBatches
    batches: BTreeMap<u64, InMemoryBatch>,

    /// Latest rows
    rows: Vec<MoonlinkRow>,

    // UNDONE(BATCH_INSERT):
    // Track uncommited disk files/ batches from big batch insert

    // Track a log of position deletions on disk_files,
    // since last iceberg snapshot
    committed_deletion_log: Vec<ProcessedDeletionRecord>,
    uncommitted_deletion_log: Vec<Option<ProcessedDeletionRecord>>,

    /// Last commit point
    last_commit: RecordLocation,

    // Next snapshot task
    //
    next_snapshot_task: SnapshotTask,
}

impl SnapshotTableState {
    fn new(metadata: Arc<TableMetadata>) -> Self {
        let mut batches = BTreeMap::new();
        batches.insert(0, InMemoryBatch::new(metadata.config.batch_size));
        Self {
            current_snapshot: Snapshot::new(metadata),
            batches,
            rows: Vec::new(),
            last_commit: RecordLocation::MemoryBatch(0, 0),
            committed_deletion_log: Vec::new(),
            uncommitted_deletion_log: Vec::new(),
            next_snapshot_task: SnapshotTask::new(),
        }
    }
}

/// Used to track the state of a streamed transaction
/// Holds the memslice and pending deletes
struct TransactionStreamState {
    mem_slice: MemSlice,
    new_deletions: Vec<RawDeletionRecord>,
}

impl TransactionStreamState {
    fn new(schema: Arc<Schema>, batch_size: usize) -> Self {
        Self {
            mem_slice: MemSlice::new(schema, batch_size),
            new_deletions: Vec::new(),
        }
    }
}

/// MooncakeTable is a disk table + mem slice.
/// Transactions will append data to the mem slice.
///
/// And periodically disk slices will be merged and compacted.
/// Single thread is used to write to the table.
///
pub struct MooncakeTable {
    /// Current metadata of the table.
    ///
    metadata: Arc<TableMetadata>,

    /// The mem slice
    ///
    mem_slice: MemSlice,

    // Current snapshot of the table
    snapshot: Arc<RwLock<SnapshotTableState>>,
    next_snapshot_task: SnapshotTask,

    // Stream state per transaction
    transaction_stream_states: HashMap<u32, TransactionStreamState>,
}

impl MooncakeTable {
    /// foreground functions
    ///
    pub fn new(schema: Schema, name: String, version: u64, base_path: PathBuf) -> Self {
        let table_config = TableConfig::new();
        let schema = Arc::new(schema);
        let metadata = Arc::new(TableMetadata {
            name,
            id: version,
            schema,
            config: table_config,
            path: base_path,
            get_lookup_key,
        });
        let table = Self {
            mem_slice: MemSlice::new(metadata.schema.clone(), metadata.config.batch_size),
            metadata: metadata.clone(),
            snapshot: Arc::new(RwLock::new(SnapshotTableState::new(metadata))),
            next_snapshot_task: SnapshotTask::new(),
            transaction_stream_states: HashMap::new(),
        };

        table
    }

    pub fn append(&mut self, row: MoonlinkRow) -> Result<()> {
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        if let Some(batch) = self.mem_slice.append(lookup_key, &row)? {
            self.next_snapshot_task.new_record_batches.push(batch);
            self.next_snapshot_task.new_rows = vec![];
        }
        self.next_snapshot_task.new_rows.push(row);
        Ok(())
    }

    pub fn delete(&mut self, row: MoonlinkRow, lsn: u64) {
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn,
            pos: None,
            _row_identity: None,
            xact_id: None,
        };
        let pos = self.mem_slice.delete(&record);
        record.pos = pos;
        self.next_snapshot_task.new_deletions.push(record);
    }

    pub fn commit(&mut self, lsn: u64) {
        self.next_snapshot_task.new_lsn = lsn;
        self.next_snapshot_task.new_commit_point = Some(self.mem_slice.get_commit_check_point());
    }

    pub fn should_flush(&self) -> bool {
        self.mem_slice.get_num_rows() >= self.metadata.config.batch_size
    }

    fn get_or_create_stream_state(&mut self, xact_id: u32) -> &mut TransactionStreamState {
        self.transaction_stream_states
            .entry(xact_id)
            .or_insert_with(|| {
                TransactionStreamState::new(
                    self.metadata.schema.clone(),
                    self.metadata.config.batch_size,
                )
            })
    }

    pub fn append_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) -> Result<()> {
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        let stream_state = self.get_or_create_stream_state(xact_id);

        stream_state.mem_slice.append(lookup_key, &row)?;

        Ok(())
    }

    pub fn delete_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) {
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn: u64::MAX, // Updated at commit time
            pos: None,
            _row_identity: None,
            xact_id: Some(xact_id),
        };

        let stream_state = self.get_or_create_stream_state(xact_id);
        let pos = stream_state.mem_slice.delete(&record);

        // This is fine since we will flush and remap to disk position on commit
        record.pos = pos;
        self.next_snapshot_task.new_deletions.push(record);
    }

    pub fn abort_in_stream_batch(&mut self, xact_id: u32) {
        // Simply remove the transaction stream state
        self.transaction_stream_states.remove(&xact_id);
    }

    fn inner_flush(
        mem_slice: &mut MemSlice,
        snapshot_task: &mut SnapshotTask,
        metadata: &Arc<TableMetadata>,
        lsn: u64,
    ) -> JoinHandle<Result<DiskSliceWriter>> {
        // Finalize the current batch (if needed)
        let (new_batch, batches, index) = mem_slice
            .flush()
            .expect("mem_slice.flush() should not fail");

        if let Some(batch) = new_batch {
            snapshot_task.new_record_batches.push(batch);
            snapshot_task.new_rows.clear();
        }

        let index = Arc::new(index);
        snapshot_task.new_mem_indices.push(index.clone());

        let mut disk_slice = DiskSliceWriter::new(
            metadata.schema.clone(),
            metadata.path.clone(),
            batches,
            lsn,
            index,
        );

        spawn(async move {
            disk_slice.write()?;
            Ok(disk_slice)
        })
    }

    pub fn flush_transaction_stream(
        &mut self,
        xact_id: u32,
        lsn: u64,
    ) -> Result<JoinHandle<Result<DiskSliceWriter>>> {
        if let Some(mut stream_state) = self.transaction_stream_states.remove(&xact_id) {
            let mem_slice = &mut stream_state.mem_slice;
            let snapshot_task = &mut self.next_snapshot_task;
            let snapshot = &mut self.snapshot;

            // We update our delete records with the last lsn of the transaction
            // Note that in the stream case we dont have this until commit time
            for deletion in stream_state.new_deletions.iter_mut() {
                deletion.lsn = lsn;
            }

            // Patch deletion records for this transaction with the commit LSN
            for deletion in snapshot_task.new_deletions.iter_mut() {
                if deletion.xact_id == Some(xact_id) {
                    deletion.lsn = lsn;
                }
            }
            for deletion in snapshot.lock().unwrap().uncommitted_deletion_log.iter_mut() {
                if let Some(deletion) = deletion {
                    if deletion.xact_id == Some(xact_id) {
                        deletion.lsn = lsn;
                    }
                }
            }

            Ok(Self::inner_flush(
                mem_slice,
                snapshot_task,
                &self.metadata,
                lsn,
            ))
        } else {
            Err(Error::TransactionNotFound(xact_id))
        }
    }

    // UNDONE(BATCH_INSERT):
    // flush uncommitted batches from big batch insert
    pub fn flush(&mut self, lsn: u64) -> JoinHandle<Result<DiskSliceWriter>> {
        Self::inner_flush(
            &mut self.mem_slice,
            &mut self.next_snapshot_task,
            &self.metadata,
            lsn,
        )
    }

    pub fn commit_flush(&mut self, disk_slice: DiskSliceWriter) -> Result<()> {
        self.next_snapshot_task.new_disk_slices.push(disk_slice);
        Ok(())
    }

    // Create a snapshot of the last committed version
    //
    pub fn create_snapshot(&mut self) -> Option<JoinHandle<()>> {
        if !self.next_snapshot_task.should_create_snapshot() {
            return None;
        }
        let mut snapshot = self.snapshot.write().unwrap();
        snapshot
            .next_snapshot_task
            .take_task(&mut self.next_snapshot_task);

        Some(spawn(Self::create_snapshot_async(self.snapshot.clone())))
    }

    pub fn request_read(&self) -> Result<(Vec<PathBuf>, Vec<(usize, usize)>)> {
        let snapshot = self.snapshot.read().unwrap();
        let mut file_paths: Vec<PathBuf> = Vec::new();
        let deletions = Self::get_deletion_records(&snapshot);
        file_paths.extend(snapshot.current_snapshot.disk_files.keys().cloned());
        let file_path = snapshot.current_snapshot.get_name_for_inmemory_file();
        if file_path.exists() {
            file_paths.push(file_path);
            return Ok((file_paths, deletions));
        }
        assert!(matches!(
            snapshot.last_commit,
            RecordLocation::MemoryBatch(_, _)
        ));
        let (batch_id, row_id) = snapshot.last_commit.clone().into();
        if batch_id > 0 || row_id > 0 {
            // add all batches
            let mut filtered_batches = Vec::new();
            let schema = self.metadata.schema.clone();
            for (id, batch) in snapshot.batches.iter() {
                if *id < batch_id {
                    if let Some(batch) = batch.get_filtered_batch().unwrap() {
                        filtered_batches.push(batch);
                    }
                } else if *id == batch_id && row_id > 0 {
                    if batch.data.is_some() {
                        let filtered_batch = batch
                            .get_filtered_batch_with_limit(row_id)
                            .unwrap()
                            .unwrap();
                        filtered_batches.push(filtered_batch);
                    } else {
                        let rows = &snapshot.rows[..row_id];
                        let deletions = &snapshot.batches.values().last().unwrap().deletions;
                        let batch = crate::storage::data_batches::create_batch_from_rows(
                            rows,
                            schema.clone(),
                            deletions,
                        );
                        filtered_batches.push(batch);
                    }
                }
            }

            if filtered_batches.len() > 0 {
                // Build a parquet file from current record batches
                //
                let mut parquet_writer =
                    ArrowWriter::try_new(std::fs::File::create(&file_path).unwrap(), schema, None)
                        .unwrap();
                for batch in filtered_batches.iter() {
                    parquet_writer.write(batch)?;
                }
                parquet_writer.close()?;
                file_paths.push(file_path);
            }
        }

        Ok((file_paths, deletions))
    }
}

// Helper functions
impl MooncakeTable {
    fn get_deletion_records(snapshot: &SnapshotTableState) -> Vec<(usize, usize)> {
        let mut ret = Vec::new();
        for deletion in snapshot.committed_deletion_log.iter() {
            if let RecordLocation::DiskFile(file_name, row_id) = &deletion.pos {
                for (id, (file, _)) in snapshot.current_snapshot.disk_files.iter().enumerate() {
                    if *file == *file_name.0 {
                        ret.push((id, *row_id));
                        break;
                    }
                }
            }
        }
        ret
    }

    async fn create_snapshot_async(snapshot_state: Arc<RwLock<SnapshotTableState>>) {
        let mut snapshot = snapshot_state.write().unwrap();
        let mut next_snapshot_task = SnapshotTask::new();
        let batch_size = snapshot.current_snapshot.metadata.config.batch_size;
        swap(&mut snapshot.next_snapshot_task, &mut next_snapshot_task);
        if next_snapshot_task.new_mem_indices.len() > 0 {
            let new_mem_indices = take(&mut next_snapshot_task.new_mem_indices);
            for mem_index in new_mem_indices {
                snapshot
                    .current_snapshot
                    .indices
                    .insert_memory_index(mem_index);
            }
        }
        if next_snapshot_task.new_record_batches.len() > 0 {
            let new_batches = take(&mut next_snapshot_task.new_record_batches);
            // previous unfinished batch is finished
            snapshot.batches.last_entry().unwrap().get_mut().data =
                Some(new_batches.first().unwrap().1.clone());
            // insert the last unfinished batch
            let last_batch_id = new_batches.last().unwrap().0 + 1;
            snapshot
                .batches
                .insert(last_batch_id, InMemoryBatch::new(batch_size));
            // copy the rest of the batches
            snapshot
                .batches
                .extend(new_batches.iter().skip(1).map(|(id, batch)| {
                    (
                        *id,
                        InMemoryBatch {
                            data: Some(batch.clone()),
                            deletions: BatchDeletionVector::new(batch.num_rows()),
                        },
                    )
                }));
            snapshot.rows.clear();
        }
        if next_snapshot_task.new_disk_slices.len() > 0 {
            let mut new_disk_slices = take(&mut next_snapshot_task.new_disk_slices);
            for slice in new_disk_slices.iter_mut() {
                snapshot.current_snapshot.disk_files.extend(
                    slice.output_files().into_iter().map(|(file, row_count)| {
                        (file.clone(), BatchDeletionVector::new(*row_count))
                    }),
                );
                let write_lsn = slice.lsn();
                let pos = snapshot
                    .committed_deletion_log
                    .partition_point(|deletion| deletion.lsn <= write_lsn);
                for entry in snapshot.committed_deletion_log.iter_mut().skip(pos) {
                    slice.remap_deletion_if_needed(entry);
                }
                for entry in snapshot.uncommitted_deletion_log.iter_mut() {
                    if let Some(deletion) = entry {
                        slice.remap_deletion_if_needed(deletion);
                    }
                }
                snapshot
                    .current_snapshot
                    .indices
                    .insert_file_index(slice.take_index().unwrap());
                snapshot
                    .current_snapshot
                    .indices
                    .delete_memory_index(slice.old_index());
                slice.input_batches().iter().for_each(|batch| {
                    snapshot.batches.remove(&batch.id);
                });
            }
        }
        if next_snapshot_task.new_rows.len() > 0 {
            let new_rows = take(&mut next_snapshot_task.new_rows);
            snapshot.rows.extend(new_rows.into_iter());
        }
        Self::process_deletion_log(&mut snapshot, &mut next_snapshot_task);
        if next_snapshot_task.new_lsn != 0 {
            snapshot.last_commit = next_snapshot_task.new_commit_point.unwrap();
            snapshot.current_snapshot.snapshot_version = next_snapshot_task.new_lsn;
        }
    }

    fn process_delete_record(
        snapshot: &mut SnapshotTableState,
        deletion: RawDeletionRecord,
    ) -> ProcessedDeletionRecord {
        if let Some(pos) = deletion.pos {
            return ProcessedDeletionRecord {
                _lookup_key: deletion.lookup_key,
                pos: pos.into(),
                lsn: deletion.lsn,
                xact_id: deletion.xact_id,
            };
        } else {
            let locations = snapshot.current_snapshot.indices.find_record(&deletion);
            for location in locations.unwrap() {
                match location {
                    RecordLocation::MemoryBatch(batch_id, row_id) => {
                        if !snapshot
                            .batches
                            .get_mut(&batch_id)
                            .unwrap()
                            .deletions
                            .is_deleted(*row_id)
                        {
                            return ProcessedDeletionRecord {
                                _lookup_key: deletion.lookup_key,
                                pos: location.clone(),
                                lsn: deletion.lsn,
                                xact_id: deletion.xact_id,
                            };
                        }
                    }
                    RecordLocation::DiskFile(file_name, row_id) => {
                        if !snapshot
                            .current_snapshot
                            .disk_files
                            .get_mut(file_name.0.as_ref())
                            .unwrap()
                            .is_deleted(*row_id)
                        {
                            return ProcessedDeletionRecord {
                                _lookup_key: deletion.lookup_key,
                                pos: location.clone(),
                                lsn: deletion.lsn,
                                xact_id: deletion.xact_id,
                            };
                        }
                    }
                }
            }
            panic!("can't find deletion record");
        }
    }

    fn commit_deletion(snapshot: &mut SnapshotTableState, deletion: ProcessedDeletionRecord) {
        match &deletion.pos {
            RecordLocation::MemoryBatch(batch_id, row_id) => {
                let res = snapshot
                    .batches
                    .get_mut(&batch_id)
                    .unwrap()
                    .deletions
                    .delete_row(*row_id);
                assert!(res);
            }
            RecordLocation::DiskFile(file_name, row_id) => {
                let res = snapshot
                    .current_snapshot
                    .disk_files
                    .get_mut(file_name.0.as_ref())
                    .unwrap()
                    .delete_row(*row_id);
                assert!(res);
            }
        }
        snapshot.committed_deletion_log.push(deletion);
    }

    fn process_deletion_log(
        snapshot: &mut SnapshotTableState,
        next_snapshot_task: &mut SnapshotTask,
    ) {
        let mut new_commited_deletion = vec![];
        snapshot.uncommitted_deletion_log.retain_mut(|deletion| {
            if deletion.as_ref().unwrap().lsn <= next_snapshot_task.new_lsn {
                new_commited_deletion.push(deletion.take().unwrap());
                false
            } else {
                true
            }
        });
        for deletion in new_commited_deletion {
            Self::commit_deletion(snapshot, deletion);
        }
        // Move committed deletions (lsn <= new_lsn) to committed deletion log
        // add raw deletion records, use index to find position and add to deletion buffer
        let new_deletions = take(&mut next_snapshot_task.new_deletions);
        // apply deletion records to deletion vectors
        for deletion in new_deletions {
            let processed_deletion = Self::process_delete_record(snapshot, deletion);
            if processed_deletion.lsn <= next_snapshot_task.new_lsn {
                Self::commit_deletion(snapshot, processed_deletion);
            } else {
                snapshot
                    .uncommitted_deletion_log
                    .push(Some(processed_deletion));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashSet;
    use std::fs::create_dir_all;
    use std::fs::File;
    use tempfile::tempdir;

    fn test_read_file(file_path: &PathBuf, expected_first_column_values: &[i32]) {
        println!("Reading file: {:?}", file_path);
        // read the generated file
        let file = File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        println!("{:?}", batch);
        println!("{:?}", batch);
        assert!(batch.num_rows() == 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual: HashSet<_> = (0..col.len()).map(|i| col.value(i)).collect();
        let expected: HashSet<_> = expected_first_column_values.iter().copied().collect();
        assert!(actual == expected);
    }
    #[tokio::test]
    async fn test_flush_operation() -> Result<()> {
        // Create a temporary directory for test data
        let temp_dir = tempdir().unwrap();
        let test_dir = temp_dir.path().join("test_flush_dir");
        create_dir_all(&test_dir).unwrap();
        println!("Test directory: {:?}", test_dir);

        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create a MooncakeTable
        let mut table =
            MooncakeTable::new(schema, "flush_test_table".to_string(), 1, test_dir.clone());

        // Append some rows
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(30),
        ]);

        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Jane".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);

        // Append rows to the table
        table.append(row1)?;
        table.append(row2)?;

        // Commit the changes
        table.commit(1);

        // Verify mem_slice has rows before flush
        assert!(
            table.mem_slice.get_num_rows() > 0,
            "Expected mem_slice to contain rows before flush"
        );

        // Execute the flush operation
        println!("Flushing table...");
        let flush_handle = table.flush(1);

        // Wait for flush to complete and get the disk slice writer
        let disk_slice = flush_handle.await.unwrap()?;

        // Commit the flush
        table.commit_flush(disk_slice)?;

        // Create a snapshot to apply the changes
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(snapshot_handle.await.is_ok(), "Snapshot creation failed");

        // Verify mem_slice is empty after flush
        assert_eq!(
            table.mem_slice.get_num_rows(),
            0,
            "Expected mem_slice to be empty after flush"
        );

        // Verify disk files were created
        let snapshot = table.snapshot.read().unwrap();
        assert!(
            snapshot.current_snapshot.disk_files.len() > 0,
            "Expected disk files to be created"
        );

        println!(
            "Disk files: {:?}",
            snapshot.current_snapshot.disk_files.keys()
        );

        // Verify the batches were moved from memory to disk
        assert_eq!(
            snapshot.batches.len(),
            1,
            "Expected only the unfinished batch to remain"
        );

        // Confirm file exists on disk
        for file_path in snapshot.current_snapshot.disk_files.keys() {
            assert!(
                file_path.exists(),
                "Expected disk file to exist: {:?}",
                file_path
            );

            // Verify file size
            let metadata = std::fs::metadata(file_path).unwrap();
            println!("File size: {} bytes", metadata.len());
            assert!(metadata.len() > 0, "Expected file to have content");
        }

        // Clean up
        drop(snapshot);
        temp_dir.close().unwrap();

        println!("Flush test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_and_read() -> Result<()> {
        // Create a temporary directory for test data
        let temp_dir = tempdir().unwrap();
        let test_dir = temp_dir.path().join("test_dir");
        create_dir_all(&test_dir).unwrap();

        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create a MooncakeTable
        let mut table = MooncakeTable::new(schema, "test_table".to_string(), 1, test_dir);

        // 1. Test append operations
        println!("Testing append operations...");

        // Append some rows
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(30),
        ]);

        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Jane".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);
        let row3 = MoonlinkRow::new(vec![
            RowValue::Int32(3),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
            RowValue::Int32(40),
        ]);

        table.append(row1)?;
        table.append(row2)?;

        // 2. Test commit operation
        println!("Testing commit operation...");
        table.commit(1);

        // 3. Test delete operation

        let row2: MoonlinkRow = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Jane".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);

        println!("Testing delete operation...");
        table.delete(row2, 2);

        // Append another row after delete
        table.append(row3)?;

        // Commit again
        table.commit(2);

        // 4. Test snapshot creation
        println!("Testing snapshot creation...");
        let snapshot_handle = table.create_snapshot().unwrap();

        // Wait for the snapshot to complete
        assert!(snapshot_handle.await.is_ok());

        // Verify snapshot was created
        let snapshot = table.snapshot.read().unwrap();
        assert!(
            snapshot.current_snapshot.snapshot_version > 0,
            "Expected read version to be updated"
        );
        println!("{:?}", snapshot.rows);
        // Drop the lock before continuing
        drop(snapshot);

        // Append more rows
        let row4 = MoonlinkRow::new(vec![
            RowValue::Int32(4),
            RowValue::ByteArray("Alice".as_bytes().to_vec()),
            RowValue::Int32(35),
        ]);

        let row5 = MoonlinkRow::new(vec![
            RowValue::Int32(5),
            RowValue::ByteArray("Charlie".as_bytes().to_vec()),
            RowValue::Int32(45),
        ]);

        table.append(row4)?;
        // Commit
        table.commit(3);

        table.append(row5)?;

        let row4 = MoonlinkRow::new(vec![
            RowValue::Int32(4),
            RowValue::ByteArray("Alice".as_bytes().to_vec()),
            RowValue::Int32(35),
        ]);
        // Delete one of the new rows
        table.delete(row4, 4);

        // Testing snapshot before commit
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(snapshot_handle.await.is_ok());
        let snapshot = table.snapshot.read().unwrap();
        assert!(snapshot.current_snapshot.snapshot_version > 0);
        drop(snapshot);

        println!("Testing read operation, should be 3 rows: [1,3,4]");
        let (file_paths, deletions) = table.request_read()?;
        assert!(!file_paths.is_empty());
        println!("Deletions: {:?}", deletions);
        let file_path = &file_paths[0];
        test_read_file(file_path, &[1, 3, 4]);

        // Commit
        table.commit(4);

        println!("Testing snapshot creation...");
        let snapshot_handle = table.create_snapshot().unwrap();

        // Wait for the snapshot to complete
        assert!(snapshot_handle.await.is_ok());

        // Verify snapshot was created
        let snapshot = table.snapshot.read().unwrap();
        assert!(
            snapshot.current_snapshot.snapshot_version > 0,
            "Expected read version to be updated"
        );
        println!("{:?}", snapshot.rows);
        println!("{:?}", snapshot.batches);

        drop(snapshot);
        // test read
        println!("Testing read operation, should be 3 rows: [1,3,5]");
        let (file_paths, deletions) = table.request_read()?;
        assert!(!file_paths.is_empty());
        println!("Deletions: {:?}", deletions);
        let file_path = &file_paths[0];
        test_read_file(file_path, &[1, 3, 5]);
        // Clean up
        temp_dir.close().unwrap();

        println!("All tests passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_iceberg_snapshot() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);
        let metadata = Arc::new(TableMetadata {
            name: "test_table".to_string(),
            id: 1,
            schema: Arc::new(schema),
            config: TableConfig::new(),
            path: PathBuf::new(),
            get_lookup_key: get_lookup_key,
        });
        let _snapshot = Snapshot::new(metadata);
        // snapshot.export_to_iceberg().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_deletions() -> Result<()> {
        // Create a temporary directory for test data
        let temp_dir = tempdir().unwrap();
        let test_dir = temp_dir.path().join("test_deletions_dir");
        create_dir_all(&test_dir).unwrap();
        println!("Test directory: {:?}", test_dir);

        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create a MooncakeTable
        let mut table =
            MooncakeTable::new(schema, "deletions_test".to_string(), 1, test_dir.clone());

        // Phase 1: Initial data load and first flush
        println!("Phase 1: Initial data load and first flush");
        let initial_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Row 1".as_bytes().to_vec()),
                RowValue::Int32(31),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Row 2".as_bytes().to_vec()),
                RowValue::Int32(32),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(3),
                RowValue::ByteArray("Row 3".as_bytes().to_vec()),
                RowValue::Int32(33),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(4),
                RowValue::ByteArray("Row 4".as_bytes().to_vec()),
                RowValue::Int32(34),
            ]),
        ];

        for row in initial_rows {
            table.append(row)?;
        }
        table.commit(1);

        // First flush
        let flush_handle = table.flush(1);
        let disk_slice = flush_handle.await.unwrap()?;
        table.commit_flush(disk_slice)?;

        // Create snapshot to apply changes
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(
            snapshot_handle.await.is_ok(),
            "Initial snapshot creation failed"
        );

        // Phase 2: Add more rows and delete some before flush
        println!("Phase 2: Add more rows and delete before flush");
        let additional_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(5),
                RowValue::ByteArray("Row 5".as_bytes().to_vec()),
                RowValue::Int32(35),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(6),
                RowValue::ByteArray("Row 6".as_bytes().to_vec()),
                RowValue::Int32(36),
            ]),
        ];

        for row in additional_rows {
            table.append(row)?;
        }

        // Delete some rows before flush
        let delete_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Row 2".as_bytes().to_vec()),
                RowValue::Int32(32),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(4),
                RowValue::ByteArray("Row 4".as_bytes().to_vec()),
                RowValue::Int32(34),
            ]),
        ];

        for row in delete_rows {
            table.delete(row, 2);
        }
        table.commit(2);

        // Create snapshot before flush to verify deletions are tracked
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(
            snapshot_handle.await.is_ok(),
            "Snapshot before flush failed"
        );

        // Verify deletions are tracked before flush
        let (file_paths, deletions) = table.request_read()?;
        assert!(!file_paths.is_empty(), "Expected files to be returned");
        assert!(
            !deletions.is_empty(),
            "Expected deletion records to be tracked"
        );
        println!("Deletion records before flush: {:?}", deletions);

        // Phase 3: Second flush and more deletions
        println!("Phase 3: Second flush and more deletions");
        let flush_handle = table.flush(2);
        let disk_slice = flush_handle.await.unwrap()?;
        table.commit_flush(disk_slice)?;

        // Create snapshot after second flush
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(
            snapshot_handle.await.is_ok(),
            "Snapshot after second flush failed"
        );

        // Add more rows and delete some from both old and new data
        let more_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(7),
                RowValue::ByteArray("Row 7".as_bytes().to_vec()),
                RowValue::Int32(37),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(8),
                RowValue::ByteArray("Row 8".as_bytes().to_vec()),
                RowValue::Int32(38),
            ]),
        ];

        for row in more_rows {
            table.append(row)?;
        }

        // Delete rows from both old and new data
        let delete_more_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Row 1".as_bytes().to_vec()),
                RowValue::Int32(31),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(6),
                RowValue::ByteArray("Row 6".as_bytes().to_vec()),
                RowValue::Int32(36),
            ]),
        ];

        for row in delete_more_rows {
            table.delete(row, 3);
        }
        table.commit(3);

        // Phase 4: Final flush and verification
        println!("Phase 4: Final flush and verification");
        let flush_handle = table.flush(3);
        let disk_slice = flush_handle.await.unwrap()?;
        table.commit_flush(disk_slice)?;

        // Create final snapshot
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(
            snapshot_handle.await.is_ok(),
            "Final snapshot creation failed"
        );

        // Verify final state
        let (file_paths, deletions) = table.request_read()?;
        assert!(!file_paths.is_empty(), "Expected files to be returned");
        println!("Final deletion records: {:?}", deletions);

        // Read and verify all files
        for file_path in file_paths {
            let file = File::open(&file_path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader = builder.build().unwrap();
            let batch = reader.next().unwrap().unwrap();

            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let age_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            println!("File: {:?}", file_path);
            println!("Rows in file:");
            for i in 0..batch.num_rows() {
                println!(
                    "Row {}: id={}, name={}, age={}",
                    i,
                    id_col.value(i),
                    name_col.value(i),
                    age_col.value(i)
                );
            }
        }

        // Clean up
        temp_dir.close().unwrap();

        println!("Deletions test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_stream_isolation() -> Result<()> {
        // Create a temporary directory for test data
        let temp_dir = tempdir().unwrap();
        let test_dir = temp_dir.path().join("test_stream_dir");
        create_dir_all(&test_dir).unwrap();
        println!("Test directory: {:?}", test_dir);

        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create a MooncakeTable
        let mut table = MooncakeTable::new(
            schema,
            "transaction_stream_test".to_string(),
            1,
            test_dir.clone(),
        );

        // Set up test scenario with multiple transactions
        println!("Phase 1: Setting up transactions");

        // Transaction 1: Will be committed
        let xact_id_1 = 101;
        let xact1_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Tx1-Row1".as_bytes().to_vec()),
                RowValue::Int32(31),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Tx1-Row2".as_bytes().to_vec()),
                RowValue::Int32(32),
            ]),
        ];

        // Transaction 2: Will be aborted
        let xact_id_2 = 102;
        let xact2_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(3),
                RowValue::ByteArray("Tx2-Row1".as_bytes().to_vec()),
                RowValue::Int32(33),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(4),
                RowValue::ByteArray("Tx2-Row2".as_bytes().to_vec()),
                RowValue::Int32(34),
            ]),
        ];

        // Transaction 3: Will modify data then commit
        let xact_id_3 = 103;
        let xact3_rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(5),
                RowValue::ByteArray("Tx3-Row1".as_bytes().to_vec()),
                RowValue::Int32(35),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(6),
                RowValue::ByteArray("Tx3-Row2".as_bytes().to_vec()),
                RowValue::Int32(36),
            ]),
        ];

        // Phase 2: Add rows to each transaction stream
        println!("Phase 2: Adding rows to transaction streams");

        // Add Transaction 1 rows
        for row in xact1_rows {
            table.append_in_stream_batch(row, xact_id_1)?;
        }

        // Add Transaction 2 rows (will be aborted)
        for row in xact2_rows {
            table.append_in_stream_batch(row, xact_id_2)?;
        }

        // Add Transaction 3 rows
        for row in xact3_rows {
            table.append_in_stream_batch(row, xact_id_3)?;
        }

        // Verify we can see the transaction states but they haven't affected the table yet
        assert_eq!(
            table.transaction_stream_states.len(),
            3,
            "Expected 3 active transaction streams"
        );

        // The main table should still be empty
        assert_eq!(
            table.mem_slice.get_num_rows(),
            0,
            "Expected main table to be empty before commits"
        );

        // Phase 3: Delete operations
        println!("Phase 3: Performing delete operations in transaction streams");

        // Delete a row from Transaction 1
        let row_to_delete = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Tx1-Row2".as_bytes().to_vec()),
            RowValue::Int32(32),
        ]);
        table.delete_in_stream_batch(row_to_delete, xact_id_1);

        // Delete a row from Transaction 3
        let row_to_delete = MoonlinkRow::new(vec![
            RowValue::Int32(5),
            RowValue::ByteArray("Tx3-Row1".as_bytes().to_vec()),
            RowValue::Int32(35),
        ]);
        table.delete_in_stream_batch(row_to_delete, xact_id_3);

        // Phase 4: Commit Transaction 1
        println!("Phase 4: Committing Transaction 1");
        // Flush the transaction data to disk
        println!("Flushing Transaction 1 data...");
        let flush_handle = table.flush_transaction_stream(xact_id_1, 1)?;

        // Wait for flush to complete and get the disk slice writer
        let disk_slice = flush_handle.await.unwrap()?;

        // Commit the flush
        table.commit_flush(disk_slice)?;

        // Create snapshot to apply changes
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(
            snapshot_handle.await.is_ok(),
            "Initial snapshot creation failed"
        );

        // Check that only Transaction 1's data is visible (and one row was deleted)
        assert_eq!(
            table.transaction_stream_states.len(),
            2,
            "Expected 2 remaining transaction streams after committing one"
        );

        // Read state - should only see Transaction 1's remaining row
        println!("Checking table state after Transaction 1 commit");
        let (file_paths, _deletions) = table.request_read()?;
        if !file_paths.is_empty() {
            let file_path = &file_paths[0];
            let file = File::open(file_path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader = builder.build().unwrap();
            let batch = reader.next().unwrap().unwrap();

            println!("Batch after Tx1 commit: {:?}", batch);

            // Should only have 1 row with ID=1 (since ID=2 was deleted)
            assert_eq!(
                batch.num_rows(),
                1,
                "Expected only one row after Transaction 1 commit"
            );
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(id_col.value(0), 1, "Expected only row ID 1 to be present");
        }

        // Phase 5: Abort Transaction 2
        println!("Phase 5: Aborting Transaction 2");
        table.abort_in_stream_batch(xact_id_2);

        // Check that Transaction 2's stream was removed
        assert_eq!(
            table.transaction_stream_states.len(),
            1,
            "Expected 1 remaining transaction stream after aborting one"
        );

        // Transaction 2's data should never appear in the table

        // Phase 6: Commit Transaction 3
        println!("Phase 6: Committing Transaction 3");

        // Add another row to Transaction 3 before committing
        let additional_row = MoonlinkRow::new(vec![
            RowValue::Int32(7),
            RowValue::ByteArray("Tx3-Row3".as_bytes().to_vec()),
            RowValue::Int32(37),
        ]);
        table.append_in_stream_batch(additional_row, xact_id_3)?;

        // Flush the transaction data to disk
        println!("Flushing Transaction 3 data...");
        let flush_handle = table.flush_transaction_stream(xact_id_3, 2)?;

        // Wait for flush to complete and get the disk slice writer
        let disk_slice = flush_handle.await.unwrap()?;

        // Commit the flush
        table.commit_flush(disk_slice)?;

        // Create snapshot to apply changes
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(
            snapshot_handle.await.is_ok(),
            "Second snapshot creation failed"
        );

        // Check that all transaction streams are gone
        assert_eq!(
            table.transaction_stream_states.len(),
            0,
            "Expected no remaining transaction streams after all are committed or aborted"
        );

        // Phase 7: Final verification
        println!("Phase 7: Verifying final table state");

        // Read the final state
        let (file_paths, _deletions) = table.request_read()?;
        assert!(!file_paths.is_empty(), "Expected files to be returned");
        println!("Final deletion records: {:?}", _deletions);

        // Collect all IDs from all batches across all files
        let mut actual_ids = HashSet::new();

        for file_path in file_paths {
            let file = File::open(&file_path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let mut reader = builder.build().unwrap();

            while let Some(Ok(batch)) = reader.next() {
                let id_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let name_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    let id = id_col.value(i);
                    let name = name_col.value(i);
                    println!("Row: id={}, name={}", id, name);
                    actual_ids.insert(id);
                }
            }
        }

        // Expected final state:
        // - Transaction 1: Row with ID=1 included, deletion of ID=2 applied
        // - Transaction 2: All rows aborted, not included
        // - Transaction 3: Rows with ID=6 and ID=7 included, deletion of ID=5 applied
        let expected_ids: HashSet<_> = [1, 6, 7].iter().copied().collect();

        assert_eq!(
            actual_ids, expected_ids,
            "Expected rows with IDs 1, 6, 7 in final table state"
        );

        // Clean up
        temp_dir.close().unwrap();

        println!("Transaction stream isolation test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_transaction_stream() -> Result<()> {
        // Create a temporary directory for test data
        let temp_dir = tempdir().unwrap();
        let test_dir = temp_dir.path().join("test_txn_flush_dir");
        create_dir_all(&test_dir).unwrap();
        println!("Test directory: {:?}", test_dir);

        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        // Create a MooncakeTable
        let mut table =
            MooncakeTable::new(schema, "txn_flush_test".to_string(), 1, test_dir.clone());

        // Set up test transaction
        let xact_id = 123;

        // Add rows to transaction
        let rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Row 1".as_bytes().to_vec()),
                RowValue::Int32(21),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Row 2".as_bytes().to_vec()),
                RowValue::Int32(22),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(3),
                RowValue::ByteArray("Row 3".as_bytes().to_vec()),
                RowValue::Int32(23),
            ]),
        ];

        for row in rows {
            table.append_in_stream_batch(row, xact_id)?;
        }

        // Delete row 2 from the transaction
        let row_to_delete = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Row 2".as_bytes().to_vec()),
            RowValue::Int32(22),
        ]);
        table.delete_in_stream_batch(row_to_delete, xact_id);

        // Verify transaction state exists before flush
        assert!(
            table.transaction_stream_states.contains_key(&xact_id),
            "Expected transaction state to exist before flush"
        );

        // Flush the transaction
        println!("Flushing transaction...");
        let flush_handle = table.flush_transaction_stream(xact_id, 1)?;

        // Verify transaction state is removed after flush
        assert!(
            !table.transaction_stream_states.contains_key(&xact_id),
            "Expected transaction state to be removed after flush"
        );

        // Wait for flush to complete and get the disk slice writer
        let disk_slice = flush_handle.await.unwrap()?;

        // Commit the flush
        table.commit_flush(disk_slice)?;

        // Create snapshot to apply changes
        let snapshot_handle = table.create_snapshot().unwrap();
        assert!(snapshot_handle.await.is_ok(), "Snapshot creation failed");

        // Verify rows are persisted
        let (file_paths, deletions) = table.request_read()?;
        assert!(!file_paths.is_empty(), "Expected files to be returned");
        println!("Deletion records: {:?}", deletions);

        // Read the file
        let file_path = &file_paths[0];
        let file = File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        // Verify the batch
        println!("Batch after flush: {:?}", batch);
        assert_eq!(
            batch.num_rows(),
            2,
            "Expected 2 rows after transaction flush (one was deleted)"
        );

        // Verify the correct rows (ID 1 and 3) are present (ID 2 was deleted)
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual_ids: HashSet<_> = (0..id_col.len()).map(|i| id_col.value(i)).collect();
        let expected_ids: HashSet<_> = [1, 3].iter().copied().collect();

        assert_eq!(
            actual_ids, expected_ids,
            "Expected only rows with IDs 1 and 3 to be present (ID 2 was deleted)"
        );

        // Clean up
        temp_dir.close().unwrap();

        println!("Transaction stream flush test passed!");
        Ok(())
    }
}
