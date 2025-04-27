use std::collections::HashSet;

use sinks::SinkError;
use sources::SourceError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use tokio_postgres::types::PgLsn;

use crate::pg_replicate::table::TableId;

pub mod batching;
pub mod sinks;
pub mod sources;

#[derive(Debug)]
pub enum PipelineAction {
    TableCopiesOnly,
    CdcOnly,
    Both,
}

pub struct PipelineResumptionState {
    pub copied_tables: HashSet<TableId>,
    pub last_lsn: PgLsn,
}

/// One per replication stream: holds the “replicated-through” LSN
/// and notifies any listeners when it moves forward.
pub struct PipelineReplicationState {
    current: AtomicU64,
    tx: watch::Sender<u64>,
}

impl PipelineReplicationState {
    /// Create a new clock initialised to LSN 0.
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = watch::channel(0);
        Arc::new(Self {
            current: AtomicU64::new(0),
            tx,
        })
    }

    /// Advance the clock if `lsn` is newer.
    pub fn mark(&self, lsn: PgLsn) {
        let lsn_u64: u64 = lsn.into();
        if lsn_u64 > self.current.load(Ordering::Relaxed) {
            self.current.store(lsn_u64, Ordering::Release);
            let _ = self.tx.send(lsn_u64); // wake any waiters; ignore closed error
        }
    }

    /// Subscribe for async notifications.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }

    /// Fast, lock-free read of the latest value.
    pub fn now(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }
}

#[derive(Debug, Error)]
pub enum PipelineError<SrcErr: SourceError, SnkErr: SinkError> {
    #[error("source error: {0}")]
    Source(#[source] SrcErr),

    #[error("sink error: {0}")]
    Sink(#[source] SnkErr),

    #[error("source error: {0}")]
    CommonSource(#[from] sources::CommonSourceError),
}
