use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::{ready, Stream};
use pin_project_lite::pin_project;
use postgres_replication::LogicalReplicationStream;
use thiserror::Error;
use tokio_postgres::{types::PgLsn, CopyOutStream};
// tracing::info is removed, we'll use println!

// Assuming ProfileGuard is in src/profiling.rs (and now uses println!)
use crate::pg_replicate::{
    clients::postgres::{ReplicationClient, ReplicationClientError}, // ReplicationClient is already profiled
    conversions::{
        cdc_event::{CdcEvent, CdcEventConversionError, CdcEventConverter}, // CdcEventConverter is already profiled
        table_row::{TableRow, TableRowConversionError, TableRowConverter},
    },
    table::{ColumnSchema, TableId, TableName, TableSchema},
};
use crate::profiling::ProfileGuard;

use super::{Source, SourceError};

pub enum TableNamesFrom {
    Vec(Vec<TableName>),
    Publication(String),
}

#[derive(Debug, Error)]
pub enum PostgresSourceError {
    #[error("cdc stream can only be started with a publication")]
    MissingPublication,

    #[error("cdc stream can only be started with a slot_name")]
    MissingSlotName,

    #[error("replication client error: {0}")]
    ReplicationClient(#[from] ReplicationClientError),

    #[error("tokio postgres error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),
}

impl SourceError for PostgresSourceError {}

pub struct PostgresSource {
    replication_client: ReplicationClient,
    table_schemas: HashMap<TableId, TableSchema>,
    slot_name: Option<String>,
    publication: Option<String>,
}

impl PostgresSource {
    pub async fn new(
        uri: &str, // Not used directly in tags here, but could be if multiple sources are configured
        slot_name: Option<String>,
        table_names_from: TableNamesFrom,
    ) -> Result<PostgresSource, PostgresSourceError> {
        let _guard = ProfileGuard::new(&format!(
            "SRC_POSTGRES_new_slot_{}",
            slot_name.as_deref().unwrap_or("none")
        ));

        // ReplicationClient methods (connect_no_tls, begin_readonly_transaction, get_or_create_slot)
        // are assumed to be profiled within ReplicationClient itself.
        let mut replication_client = ReplicationClient::connect_no_tls(uri).await?;
        replication_client.begin_readonly_transaction().await?;

        if let Some(ref s_name) = slot_name {
            // Renamed to avoid conflict with outer slot_name
            let _slot_guard =
                ProfileGuard::new(&format!("SRC_POSTGRES_new_get_or_create_slot_{}", s_name));
            replication_client.get_or_create_slot(s_name).await?;
        }

        let (table_names, publication_opt) = {
            // Renamed publication to publication_opt
            let _tn_guard = ProfileGuard::new("SRC_POSTGRES_new_get_table_names_and_publication");
            Self::get_table_names_and_publication(&replication_client, table_names_from).await?
        };

        let table_schemas = {
            // replication_client.get_table_schemas is already profiled internally
            let _ts_guard = ProfileGuard::new(&format!(
                "SRC_POSTGRES_new_get_table_schemas_count_{}_pub_{}",
                table_names.len(),
                publication_opt.as_deref().unwrap_or("none")
            ));
            replication_client
                .get_table_schemas(&table_names, publication_opt.as_deref())
                .await?
        };

        Ok(PostgresSource {
            replication_client,
            table_schemas,
            publication: publication_opt, // Use the renamed variable
            slot_name,
        })
    }

    fn publication(&self) -> Option<&String> {
        self.publication.as_ref()
    }

    fn slot_name(&self) -> Option<&String> {
        self.slot_name.as_ref()
    }

    async fn get_table_names_and_publication(
        replication_client: &ReplicationClient,
        table_names_from: TableNamesFrom,
    ) -> Result<(Vec<TableName>, Option<String>), ReplicationClientError> {
        // This function is called within `new`, its _tn_guard in `new` covers its overall time.
        // Specific calls to replication_client inside are already profiled there.
        Ok(match table_names_from {
            TableNamesFrom::Vec(table_names) => (table_names, None),
            TableNamesFrom::Publication(publication_name) => {
                // Renamed publication
                let _pub_exists_guard = ProfileGuard::new_with_prefix(
                    "SRC_POSTGRES_get_table_names_pub_exists",
                    &publication_name,
                );
                if !replication_client
                    .publication_exists(&publication_name)
                    .await?
                {
                    return Err(ReplicationClientError::MissingPublication(
                        publication_name.to_string(),
                    ));
                }
                let _get_names_guard = ProfileGuard::new_with_prefix(
                    "SRC_POSTGRES_get_table_names_from_pub",
                    &publication_name,
                );
                (
                    replication_client
                        .get_publication_table_names(&publication_name)
                        .await?,
                    Some(publication_name),
                )
            }
        })
    }
}

#[async_trait]
impl Source for PostgresSource {
    type Error = PostgresSourceError;

    fn get_table_schemas(&self) -> &HashMap<TableId, TableSchema> {
        // This is a cheap getter, no profiling needed.
        &self.table_schemas
    }

    async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<TableCopyStream, Self::Error> {
        let _guard = ProfileGuard::new_with_prefix(
            "SRC_POSTGRES_get_table_copy_stream",
            &table_name.to_string(),
        );
        // Replaced tracing::info with println!
        println!("starting table copy stream for table {}", table_name);

        // self.replication_client.get_table_copy_stream is already profiled internally
        let stream = self
            .replication_client
            .get_table_copy_stream(table_name, column_schemas)
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;

        Ok(TableCopyStream {
            stream,
            column_schemas: column_schemas.to_vec(),
        })
    }

    async fn commit_transaction(&mut self) -> Result<(), Self::Error> {
        let _guard = ProfileGuard::new("SRC_POSTGRES_commit_transaction");
        // self.replication_client.commit_txn is already profiled internally
        self.replication_client
            .commit_txn()
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;
        Ok(())
    }

    async fn get_cdc_stream(&self, start_lsn: PgLsn) -> Result<CdcStream, Self::Error> {
        let _guard = ProfileGuard::new(&format!("SRC_POSTGRES_get_cdc_stream_lsn_{}", start_lsn));
        // Replaced tracing::info with println!
        println!("starting cdc stream at lsn {}", start_lsn);

        let publication = self
            .publication()
            .ok_or(PostgresSourceError::MissingPublication)?;
        let slot_name = self
            .slot_name()
            .ok_or(PostgresSourceError::MissingSlotName)?;

        // self.replication_client.get_logical_replication_stream is already profiled internally
        let stream = self
            .replication_client
            .get_logical_replication_stream(publication, slot_name, start_lsn)
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;

        const TIME_SEC_CONVERSION: u64 = 946_684_800;
        let postgres_epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        Ok(CdcStream {
            stream,
            table_schemas: self.table_schemas.clone(), // Cloning HashMap, could be a point of interest if huge
            postgres_epoch,
        })
    }
}

#[derive(Debug, Error)]
pub enum TableCopyStreamError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("conversion error: {0}")]
    ConversionError(TableRowConversionError),
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream {
        #[pin]
        stream: CopyOutStream,
        column_schemas: Vec<ColumnSchema>, // Cloning Vec<ColumnSchema> in get_table_copy_stream, could be point of interest
    }
}

impl Stream for TableCopyStream {
    type Item = Result<TableRow, TableCopyStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let _guard = ProfileGuard::new("SRC_POSTGRES_TableCopyStream_poll_next");
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(row_bytes)) => {
                // Renamed from row to row_bytes for clarity
                // TableRowConverter::try_from is synchronous. Its time is part of poll_next.
                // If this conversion is complex and poll_next is slow, it might be a candidate for
                // more granular profiling or optimization of the converter itself.
                // For now, the overall poll_next time will capture it.
                let _conversion_guard =
                    ProfileGuard::new("SRC_POSTGRES_TableCopyStream_poll_next_conversion");
                match TableRowConverter::try_from(&row_bytes, this.column_schemas) {
                    Ok(table_row) => Poll::Ready(Some(Ok(table_row))), // Renamed
                    Err(e) => {
                        let e = TableCopyStreamError::ConversionError(e);
                        Poll::Ready(Some(Err(e)))
                    }
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug, Error)]
pub enum CdcStreamError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("cdc event conversion error: {0}")]
    CdcEventConversion(#[from] CdcEventConversionError),
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct CdcStream {
        #[pin]
        stream: LogicalReplicationStream, // This is the stream from postgres_replication crate
        table_schemas: HashMap<TableId, TableSchema>,
        postgres_epoch: SystemTime,
    }
}

#[derive(Debug, Error)]
pub enum StatusUpdateError {
    #[error("system time error: {0}")]
    SystemTime(#[from] SystemTimeError),

    #[error("tokio_postgres error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),
}

impl CdcStream {
    pub async fn send_status_update(
        self: Pin<&mut Self>,
        lsn: PgLsn,
    ) -> Result<(), StatusUpdateError> {
        let _guard = ProfileGuard::new(&format!(
            "SRC_POSTGRES_CdcStream_send_status_update_lsn_{}",
            lsn
        ));
        let this = self.project();
        let ts = this.postgres_epoch.elapsed()?.as_micros() as i64; // SystemTime::elapsed can be profiled if it shows up

        // The actual await on standby_status_update is the main work here.
        // If this function is slow, it's likely due to this I/O.
        this.stream
            .standby_status_update(lsn, lsn, lsn, ts, 0)
            .await?;

        Ok(())
    }
}

impl Stream for CdcStream {
    type Item = Result<CdcEvent, CdcStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let _guard = ProfileGuard::new("SRC_POSTGRES_CdcStream_poll_next");
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(msg)) => {
                // CdcEventConverter::try_from is already profiled internally.
                // The time spent here will be captured by _guard.
                let _conversion_guard =
                    ProfileGuard::new("SRC_POSTGRES_CdcStream_poll_next_conversion");
                match CdcEventConverter::try_from(msg, this.table_schemas) {
                    Ok(cdc_event) => Poll::Ready(Some(Ok(cdc_event))), // Renamed
                    Err(e) => Poll::Ready(Some(Err(e.into()))),
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
}
