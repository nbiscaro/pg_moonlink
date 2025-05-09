use crate::storage::index::*; // Assuming this brings in MemIndex, FileIndex, IndexPtr if needed
use crate::storage::storage_utils::{RawDeletionRecord, RecordLocation};
use std::collections::HashSet;
use std::sync::Arc;

// Add ProfileGuard
use crate::profiling::ProfileGuard;

impl<'a> Index<'a> for MemIndex {
    type ReturnType = &'a RecordLocation;
    fn find_record(&'a self, raw_record: &RawDeletionRecord) -> Option<Vec<&'a RecordLocation>> {
        // This is a HashMap get, very fast. Not profiling unless MemIndex itself becomes complex.
        self.get_vec(&raw_record.lookup_key)
            .map(|v| v.iter().collect())
    }
}

impl MooncakeIndex {
    /// Create a new, empty in-memory index
    pub fn new() -> Self {
        // new() is cheap.
        Self {
            in_memory_index: HashSet::new(),
            file_indices: Vec::new(),
        }
    }

    /// Insert a memory index (batch of in-memory records)
    pub fn insert_memory_index(&mut self, mem_index: Arc<MemIndex>) {
        // HashSet insert is fast.
        let _guard = ProfileGuard::new("MOONCAKE_INDEX_insert_memory_index");
        self.in_memory_index.insert(IndexPtr(mem_index));
    }

    pub fn delete_memory_index(&mut self, mem_index: &Arc<MemIndex>) {
        // HashSet remove is fast.
        let _guard = ProfileGuard::new("MOONCAKE_INDEX_delete_memory_index");
        self.in_memory_index.remove(&IndexPtr(mem_index.clone()));
    }

    /// Insert a file index (batch of on-disk records)
    pub fn insert_file_index(&mut self, file_index: FileIndex) {
        // Vec push is fast.
        let _guard = ProfileGuard::new("MOONCAKE_INDEX_insert_file_index");
        self.file_indices.push(file_index);
    }
}

impl<'a> Index<'a> for MooncakeIndex {
    type ReturnType = RecordLocation;
    // This is the primary search function for MooncakeIndex.
    fn find_record(&'a self, raw_record: &RawDeletionRecord) -> Option<Vec<RecordLocation>> {
        let _guard = ProfileGuard::new(&format!(
            "MOONCAKE_INDEX_find_record_key_{}_mem_indices_{}_file_indices_{}",
            raw_record.lookup_key,
            self.in_memory_index.len(),
            self.file_indices.len()
        ));
        let mut res: Vec<RecordLocation> = Vec::new();

        // Check in-memory indices
        // The cost depends on the number of MemIndex instances. Each get_vec is fast.
        {
            let _mem_search_guard = ProfileGuard::new(&format!(
                "MOONCAKE_INDEX_find_record_search_mem_indices_count_{}",
                self.in_memory_index.len()
            ));
            for index_ptr in self.in_memory_index.iter() {
                // Renamed index to index_ptr
                // index_ptr.0.get_vec is a HashMap lookup.
                if let Some(locations) = index_ptr.0.get_vec(&raw_record.lookup_key) {
                    res.extend(locations.iter().cloned());
                }
            }
        }

        // Check file indices
        // The cost depends on the number of FileIndex instances and the efficiency of their search methods.
        {
            let _file_search_guard = ProfileGuard::new(&format!(
                "MOONCAKE_INDEX_find_record_search_file_indices_count_{}",
                self.file_indices.len()
            ));
            for file_index_meta in &self.file_indices {
                // file_index_meta.search (e.g., GlobalIndex.search) is already profiled.
                // This extend will show how many results are aggregated.
                let locations_from_file = {
                    // Renamed to avoid conflict
                    let _one_file_search_guard =
                        ProfileGuard::new("MOONCAKE_INDEX_find_record_search_one_file_index");
                    file_index_meta.search(&raw_record.lookup_key)
                };
                res.extend(locations_from_file);
            }
        }

        if res.is_empty() {
            None
        } else {
            Some(res)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_in_memory_index_basic() {
        let mut index = MooncakeIndex::new();

        // Insert memory records as a batch
        let mut mem_index = MemIndex::new();
        mem_index.insert(1, RecordLocation::MemoryBatch(0, 5));
        mem_index.insert(2, RecordLocation::MemoryBatch(0, 10));
        mem_index.insert(3, RecordLocation::MemoryBatch(1, 3));
        index.insert_memory_index(Arc::new(mem_index));

        let record = RawDeletionRecord {
            lookup_key: 1,
            row_identity: None,
            pos: None,
            lsn: 1,
        };

        // Test the Index trait implementation
        let trait_locations = index.find_record(&record);
        assert!(trait_locations.is_some());
        assert_eq!(trait_locations.unwrap().len(), 1);
    }
}
