use super::data_batches::BatchEntry;
use crate::error::{Error, Result};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::index::{FileIndex, MemIndex};
use crate::storage::storage_utils::{FileId, ProcessedDeletionRecord, RecordLocation};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter; // ArrowWriter::write and close are key I/O points
use std::collections::HashMap;
use std::fs::File; // File::create
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

// Add ProfileGuard
use crate::profiling::ProfileGuard;

// TODO(hjiang): Split into two structs, DiskSliceWriter and DiskSlice.
pub(crate) struct DiskSliceWriter {
    schema: Arc<Schema>,
    dir_path: PathBuf,
    batches: Vec<BatchEntry>,
    writer_lsn: Option<u64>,
    old_index: Arc<MemIndex>,
    batch_id_to_idx: HashMap<u64, usize>,
    row_offset_mapping: Vec<Vec<Option<(usize, usize)>>>,
    new_index: Option<FileIndex>,
    files: Vec<(PathBuf, usize)>,
}

impl DiskSliceWriter {
    #[cfg(debug_assertions)]
    const PARQUET_FILE_SIZE: usize = 1024 * 1024 * 2; // 2MB

    #[cfg(not(debug_assertions))]
    const PARQUET_FILE_SIZE: usize = 1024 * 1024 * 128; // 128MB

    pub(super) fn new(
        schema: Arc<Schema>,
        dir_path: PathBuf,
        batches: Vec<BatchEntry>,
        writer_lsn: Option<u64>,
        old_index: Arc<MemIndex>,
    ) -> Self {
        // new() is cheap, mostly struct initialization.
        Self {
            schema,
            dir_path,
            batches,
            files: vec![],
            batch_id_to_idx: HashMap::new(),
            writer_lsn,
            row_offset_mapping: vec![],
            old_index,
            new_index: None,
        }
    }

    /// Apply deletion vector to in-memory batches, write to parquet files and remap index.
    pub(super) fn write(&mut self) -> Result<()> {
        // This is the main orchestrator for disk writing.
        let _guard = ProfileGuard::new(&format!(
            "DS_WRITER_write_dir_{:?}_lsn_{:?}_batches_{}",
            self.dir_path,
            self.writer_lsn,
            self.batches.len()
        ));

        let mut filtered_batches = Vec::new();
        let mut id = 0;
        // Part 1: Filtering batches based on deletions. This is CPU-bound.
        {
            let _filter_guard = ProfileGuard::new(&format!(
                "DS_WRITER_write_filter_batches_count_{}",
                self.batches.len()
            ));
            for entry in self.batches.iter() {
                // entry.batch.get_filtered_batch() could be significant if batches are large/deletions complex.
                let filtered_batch_opt = {
                    let _get_filtered_guard = ProfileGuard::new(&format!(
                        "DS_WRITER_write_get_filtered_batch_id_{}",
                        entry.id
                    ));
                    entry.batch.get_filtered_batch()?
                };

                if let Some(batch_val) = filtered_batch_opt {
                    // Renamed
                    let total_rows = entry.batch.data.as_ref().unwrap().num_rows();
                    filtered_batches.push((
                        id,
                        batch_val, // Use renamed
                        entry.batch.deletions.collect_active_rows(total_rows),
                    ));
                    let mut mapping = Vec::with_capacity(total_rows);
                    mapping.resize(total_rows, None);
                    self.row_offset_mapping.push(mapping);
                    self.batch_id_to_idx.insert(entry.id, id);
                    id += 1;
                }
            }
        }

        // Part 2: Writing to Parquet. This involves I/O.
        // self.write_batch_to_parquet is profiled internally.
        self.write_batch_to_parquet(&filtered_batches)?;

        // Part 3: Remapping index. This is CPU-bound.
        // self.remap_index is profiled internally.
        self.remap_index()?;
        Ok(())
    }

    pub(super) fn lsn(&self) -> Option<u64> {
        self.writer_lsn // cheap
    }

    pub(super) fn set_lsn(&mut self, lsn: Option<u64>) {
        self.writer_lsn = lsn; // cheap
    }

    pub(super) fn input_batches(&self) -> &Vec<BatchEntry> {
        &self.batches // cheap
    }

    pub(super) fn output_files(&self) -> &[(PathBuf, usize)] {
        self.files.as_slice() // cheap
    }

    pub(super) fn old_index(&self) -> &Arc<MemIndex> {
        &self.old_index // cheap
    }

    /// Write record batches to parquet files in synchronous mode.
    fn write_batch_to_parquet(
        &mut self,
        record_batches: &Vec<(usize, RecordBatch, Vec<usize>)>,
    ) -> Result<()> {
        let _guard = ProfileGuard::new(&format!(
            "DS_WRITER_write_batch_to_parquet_num_input_batches_{}",
            record_batches.len()
        ));
        let mut files_vec = Vec::new(); // Renamed from files
        let mut current_writer = None; // Renamed from writer
        let mut out_file_idx = 0;
        let mut out_row_idx = 0;
        let dir_path_ref = &self.dir_path; // Renamed from dir_path
        let mut current_file_path = None; // Renamed from file_path

        for (batch_id, batch_ref, row_indices) in record_batches {
            // Renamed from batch
            if current_writer.is_none() {
                let _file_create_guard =
                    ProfileGuard::new("DS_WRITER_parquet_create_file_and_writer");
                let file_name = format!("{}.parquet", Uuid::new_v4());
                current_file_path = Some(dir_path_ref.join(file_name));
                let file_obj =
                    File::create(current_file_path.as_ref().unwrap()).map_err(Error::Io)?; // Renamed
                out_file_idx = files_vec.len();
                current_writer = Some(ArrowWriter::try_new(file_obj, self.schema.clone(), None)?);
                out_row_idx = 0;
            }

            // Row offset mapping is CPU work.
            {
                let _offset_mapping_guard = ProfileGuard::new(&format!(
                    "DS_WRITER_parquet_offset_mapping_batch_id_{}_rows_{}",
                    batch_id,
                    row_indices.len()
                ));
                for row_idx_ref in row_indices {
                    // Renamed from row_idx
                    self.row_offset_mapping[*batch_id][*row_idx_ref] =
                        Some((out_file_idx, out_row_idx));
                    out_row_idx += 1;
                }
            }

            // Actual Parquet write.
            {
                let _parquet_write_call_guard = ProfileGuard::new(&format!(
                    "DS_WRITER_parquet_ArrowWriter_write_batch_id_{}",
                    batch_id
                ));
                current_writer.as_mut().unwrap().write(batch_ref)?;
            }

            // Check if file size limit is reached.
            if current_writer.as_ref().unwrap().memory_size() > Self::PARQUET_FILE_SIZE {
                let _parquet_close_guard =
                    ProfileGuard::new("DS_WRITER_parquet_ArrowWriter_close_due_to_size");
                current_writer.unwrap().close()?; // This flushes to disk.
                current_writer = None;
                files_vec.push((current_file_path.unwrap(), out_row_idx));
                current_file_path = None;
            }
        }

        // Close the last writer if it exists.
        if let Some(writer_val) = current_writer {
            // Renamed from writer
            let _parquet_final_close_guard =
                ProfileGuard::new("DS_WRITER_parquet_ArrowWriter_final_close");
            writer_val.close()?; // This flushes to disk.
            println!("flush parquet file to local disk slice"); // Existing log
            files_vec.push((current_file_path.unwrap(), out_row_idx));
        }
        self.files = files_vec;
        Ok(())
    }

    fn remap_index(&mut self) -> Result<()> {
        let _guard = ProfileGuard::new("DS_WRITER_remap_index");
        // Iterating and filtering the old index. CPU-bound.
        let list_val = {
            // Renamed
            let _iter_filter_guard =
                ProfileGuard::new("DS_WRITER_remap_index_iter_filter_old_index");
            self.old_index
                .iter()
                .filter_map(|(key, value)| {
                    let RecordLocation::MemoryBatch(batch_id, row_idx) = value else {
                        panic!("Invalid record location");
                    };
                    let old_location = (*self.batch_id_to_idx.get(batch_id).unwrap(), *row_idx);
                    let new_location_opt = self.row_offset_mapping[old_location.0][old_location.1]; // Renamed
                    new_location_opt.map(|new_loc| (*key, new_loc.0, new_loc.1))
                    // Renamed
                })
                .collect::<Vec<_>>()
        };

        // Building the new global index. CPU-bound, involves hash map construction.
        let new_file_index = {
            // Renamed
            let _build_index_guard =
                ProfileGuard::new("DS_WRITER_remap_index_GlobalIndexBuilder_build");
            let mut index_builder = GlobalIndexBuilder::new();
            index_builder.set_files(
                self.files
                    .iter()
                    .map(|(path, _)| Arc::new(path.clone()))
                    .collect(),
            );
            index_builder.set_directory(self.dir_path.clone());
            index_builder.build_from_flush(list_val) // Use renamed
        };
        self.new_index = Some(new_file_index);
        Ok(())
    }

    pub fn take_index(&mut self) -> Option<FileIndex> {
        self.new_index.take() // cheap
    }

    pub fn remap_deletion_if_needed(&self, deletion: &mut ProcessedDeletionRecord) {
        // This is a synchronous, conditional update. Usually fast.
        // Not profiling unless it becomes a high-frequency hot spot.
        if let RecordLocation::MemoryBatch(batch_id, row_idx) = &deletion.pos {
            let batch_was_flushed = self.batch_id_to_idx.contains_key(batch_id);
            if batch_was_flushed {
                let old_location = (*self.batch_id_to_idx.get(batch_id).unwrap(), *row_idx);
                let new_location_opt = self.row_offset_mapping[old_location.0][old_location.1]; // Renamed
                if let Some(new_loc) = new_location_opt {
                    // Renamed
                    deletion.pos = RecordLocation::DiskFile(
                        FileId(Arc::new(self.files[new_loc.0].0.clone())),
                        new_loc.1,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{Identity, MoonlinkRow, RowValue};
    use crate::storage::mooncake_table::mem_slice::MemSlice;
    use crate::storage::storage_utils::RawDeletionRecord;
    use arrow::datatypes::{DataType, Field};
    use arrow_schema::Schema;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    #[test]
    fn test_disk_slice_builder() -> Result<()> {
        let temp_dir = tempdir().map_err(Error::Io)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
        ]));
        let mut mem_slice = MemSlice::new(schema.clone(), 100);
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("Alice".as_bytes().to_vec()),
        ]);
        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
        ]);
        mem_slice.append(1, row1)?;
        mem_slice.append(2, row2)?;
        let (_new_batch, entries, _index) = mem_slice.drain().unwrap();
        let mut old_index = MemIndex::new();
        old_index.insert(1, RecordLocation::MemoryBatch(0, 0));
        old_index.insert(2, RecordLocation::MemoryBatch(0, 1));
        let mut disk_slice = DiskSliceWriter::new(
            schema,
            temp_dir.path().to_path_buf(),
            entries,
            Some(1),
            Arc::new(old_index),
        );
        disk_slice.write()?;
        assert!(!disk_slice.output_files().is_empty());
        println!("Files: {:?}", disk_slice.output_files());
        for (file, _rows) in disk_slice.output_files() {
            let file = File::open(file).map_err(Error::Io)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            println!("Converted arrow schema is: {}", builder.schema());
            let mut reader = builder.build().unwrap();
            let record_batch = reader.next().unwrap().unwrap();
            println!("{:?}", record_batch);
        }
        temp_dir.close().map_err(Error::Io)?;
        Ok(())
    }

    #[test]
    fn test_index_remapping() -> Result<()> {
        let temp_dir = tempdir().map_err(Error::Io)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
        ]));
        let mut mem_slice = MemSlice::new(schema.clone(), 3);
        let rows = [
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Alice".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Bob".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(3),
                RowValue::ByteArray("Charlie".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(4),
                RowValue::ByteArray("David".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(5),
                RowValue::ByteArray("Eve".as_bytes().to_vec()),
            ]),
        ];
        for row in rows.into_iter() {
            let key = match row.values[0] {
                RowValue::Int32(v) => v as u64,
                _ => panic!("Expected i32"),
            };
            mem_slice.append(key, row)?;
        }
        mem_slice.delete(
            &RawDeletionRecord {
                lookup_key: 2,
                row_identity: None,
                pos: Some((0, 1)),
                lsn: 1,
            },
            &Identity::SinglePrimitiveKey(0),
        );
        mem_slice.delete(
            &RawDeletionRecord {
                lookup_key: 4,
                row_identity: None,
                pos: Some((0, 3)),
                lsn: 1,
            },
            &Identity::SinglePrimitiveKey(0),
        );
        let (_new_batch, entries, index) = mem_slice.drain().unwrap();
        let mut disk_slice = DiskSliceWriter::new(
            schema,
            temp_dir.path().to_path_buf(),
            entries,
            Some(1),
            Arc::new(index),
        );
        disk_slice.write()?;
        assert!(!disk_slice.output_files().is_empty());
        println!("Files created: {:?}", disk_slice.output_files());
        let new_index = disk_slice.take_index().unwrap();
        for key in [1, 3, 5] {
            let locations = new_index.search(&key);
            assert!(
                !locations.is_empty(),
                "Key {key} should exist in the remapped index"
            );
            for location in locations {
                match location {
                    RecordLocation::DiskFile(file_id, _) => {
                        let file_path = &file_id.0;
                        assert!(
                            disk_slice
                                .output_files()
                                .iter()
                                .any(|(path, _)| path == file_path.as_ref()),
                            "Referenced file path should exist in output files"
                        );
                    }
                    _ => panic!("Expected DiskFile location, found: {:?}", location),
                }
            }
        }
        for key in [2, 4] {
            let locations = new_index.search(&key);
            assert!(
                locations.is_empty(),
                "Deleted key {key} should not exist in the remapped index"
            );
        }
        temp_dir.close().map_err(Error::Io)?;
        Ok(())
    }
}
