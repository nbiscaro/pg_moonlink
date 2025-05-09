use std::{collections::HashSet, pin::Pin, time::Instant};

use futures::{stream::Stream, StreamExt};
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

// Add ProfileGuard
use crate::pg_replicate::{
    conversions::cdc_event::{CdcEvent, CdcEventConversionError},
    pipeline::{
        batching::stream::BatchTimeoutStream,
        sinks::BatchSink,
        sources::{postgres::CdcStreamError, CommonSourceError, Source},
        PipelineAction, PipelineError,
    },
    table::TableId,
};
use crate::profiling::ProfileGuard;

use super::BatchConfig;

pub struct BatchDataPipeline<Src: Source, Snk: BatchSink> {
    source: Src,
    sink: Snk,
    action: PipelineAction,
    batch_config: BatchConfig,
}

impl<Src: Source, Snk: BatchSink> BatchDataPipeline<Src, Snk> {
    pub fn new(source: Src, sink: Snk, action: PipelineAction, batch_config: BatchConfig) -> Self {
        // new() is typically cheap, not profiling for now unless requested.
        BatchDataPipeline {
            source,
            sink,
            action,
            batch_config,
        }
    }

    async fn copy_table_schemas(&mut self) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let _guard = ProfileGuard::new("PIPELINE_copy_table_schemas");
        let table_schemas = self.source.get_table_schemas();
        let table_schemas = table_schemas.clone();

        if !table_schemas.is_empty() {
            let _sink_write_guard = ProfileGuard::new("PIPELINE_copy_table_schemas_sink_write");
            self.sink
                .write_table_schemas(table_schemas)
                .await
                .map_err(PipelineError::Sink)?;
        }

        Ok(())
    }

    async fn copy_tables(
        &mut self,
        copied_tables: &HashSet<TableId>,
    ) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let _guard_overall_copy_tables = ProfileGuard::new("PIPELINE_copy_tables_overall");
        let start = Instant::now(); // Existing timing
        let table_schemas = self.source.get_table_schemas();

        let mut keys: Vec<u32> = table_schemas.keys().copied().collect();
        keys.sort();

        for key in keys {
            let table_schema = table_schemas.get(&key).expect("failed to get table key");
            let _table_copy_guard = ProfileGuard::new_with_prefix(
                "PIPELINE_copy_single_table",
                &table_schema.table_name.to_string(),
            );

            if copied_tables.contains(&table_schema.table_id) {
                info!("table {} already copied.", table_schema.table_name); // Existing log
                continue;
            }

            {
                let _truncate_guard = ProfileGuard::new(&format!(
                    "PIPELINE_copy_single_table_truncate_{}",
                    table_schema.table_name
                ));
                self.sink
                    .truncate_table(table_schema.table_id)
                    .await
                    .map_err(PipelineError::Sink)?;
            }

            let table_rows_stream_val = {
                // Renamed to avoid conflict with _guard
                let _stream_setup_guard = ProfileGuard::new(&format!(
                    "PIPELINE_copy_single_table_get_stream_{}",
                    table_schema.table_name
                ));
                self.source
                    .get_table_copy_stream(&table_schema.table_name, &table_schema.column_schemas)
                    .await
                    .map_err(PipelineError::Source)?
            };

            let batch_timeout_stream =
                BatchTimeoutStream::new(table_rows_stream_val, self.batch_config.clone());

            pin!(batch_timeout_stream);

            let mut batch_idx_counter = 0; // For unique batch tags
            while let Some(batch) = batch_timeout_stream.next().await {
                batch_idx_counter += 1;
                let _batch_process_guard = ProfileGuard::new(&format!(
                    "PIPELINE_copy_single_table_{}_process_batch_{}",
                    table_schema.table_name, batch_idx_counter
                ));
                info!("got {} table copy events in a batch", batch.len()); // Existing log
                                                                           //TODO: Avoid a vec copy // Existing TODO
                let rows_vec = {
                    // Renamed to avoid conflict with _guard
                    let _map_err_guard = ProfileGuard::new(&format!(
                        "PIPELINE_copy_single_table_{}_batch_{}_map_errors",
                        table_schema.table_name, batch_idx_counter
                    ));
                    let mut rows = Vec::with_capacity(batch.len());
                    for row_result in batch {
                        // Renamed from 'row' to 'row_result'
                        rows.push(row_result.map_err(CommonSourceError::TableCopyStream)?);
                    }
                    rows
                };
                {
                    let _sink_write_rows_guard = ProfileGuard::new(&format!(
                        "PIPELINE_copy_single_table_{}_batch_{}_sink_write_rows_count_{}",
                        table_schema.table_name,
                        batch_idx_counter,
                        rows_vec.len()
                    ));
                    self.sink
                        .write_table_rows(rows_vec, table_schema.table_id)
                        .await
                        .map_err(PipelineError::Sink)?;
                }
            }

            {
                let _mark_copied_guard = ProfileGuard::new(&format!(
                    "PIPELINE_copy_single_table_mark_copied_{}",
                    table_schema.table_name
                ));
                self.sink
                    .table_copied(table_schema.table_id)
                    .await
                    .map_err(PipelineError::Sink)?;
            }
        }
        {
            let _commit_guard = ProfileGuard::new("PIPELINE_copy_tables_source_commit_transaction");
            self.source
                .commit_transaction()
                .await
                .map_err(PipelineError::Source)?;
        }

        let end = Instant::now(); // Existing timing
        let seconds = (end - start).as_secs(); // Existing timing
        debug!("took {seconds} seconds to copy tables"); // Existing log

        Ok(())
    }

    async fn copy_cdc_events(
        &mut self,
        last_lsn_input: PgLsn, // Renamed to avoid conflict with internal mutable last_lsn
    ) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let _guard_overall_cdc_events = ProfileGuard::new("PIPELINE_copy_cdc_events_overall");
        {
            let _commit_guard =
                ProfileGuard::new("PIPELINE_cdc_source_commit_transaction_pre_stream");
            self.source
                .commit_transaction()
                .await
                .map_err(PipelineError::Source)?;
        }

        let mut last_lsn_val: u64 = last_lsn_input.into(); // Use renamed input
        last_lsn_val += 1;
        let cdc_events_stream = {
            // Renamed to avoid conflict
            let _stream_setup_guard = ProfileGuard::new(&format!(
                "PIPELINE_cdc_get_stream_from_lsn_{}",
                last_lsn_val
            ));
            self.source
                .get_cdc_stream(last_lsn_val.into())
                .await
                .map_err(PipelineError::Source)?
        };

        pin!(cdc_events_stream);

        let mut event_idx_counter = 0; // For unique event tags
        while let Some(event_result) = cdc_events_stream.next().await {
            // Renamed from 'event' to 'event_result'
            event_idx_counter += 1;
            let _event_process_guard = ProfileGuard::new(&format!(
                "PIPELINE_cdc_process_event_num_{}",
                event_idx_counter
            ));

            let mut send_status_update = false;
            if let Err(CdcStreamError::CdcEventConversion(
                CdcEventConversionError::MissingSchema(_),
            )) = event_result
            // Check before map_err
            {
                continue;
            }
            let event_val = event_result.map_err(CommonSourceError::CdcStream)?; // Renamed from 'event'
            match &event_val {
                CdcEvent::PrimaryKeepAlive(primary_keepalive_body) => {
                    send_status_update = primary_keepalive_body.reply() == 1;
                }
                _ => {}
            }
            let returned_lsn = {
                // Renamed from 'last_lsn' to avoid conflict
                let _sink_write_cdc_guard = ProfileGuard::new(&format!(
                    "PIPELINE_cdc_sink_write_event_num_{}",
                    event_idx_counter
                ));
                self.sink
                    .write_cdc_event(event_val)
                    .await
                    .map_err(PipelineError::Sink)?
            };
            if send_status_update {
                let _status_update_guard = ProfileGuard::new(&format!(
                    "PIPELINE_cdc_send_status_update_lsn_{}",
                    returned_lsn
                ));
                info!("sending status update with lsn: {returned_lsn}"); // Existing log
                cdc_events_stream
                    .as_mut()
                    .send_status_update(returned_lsn)
                    .await;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), PipelineError<Src::Error, Snk::Error>> {
        let _guard_overall_start =
            ProfileGuard::new_with_prefix("PIPELINE_start_action", &format!("{:?}", self.action));

        let resumption_state_val = {
            // Renamed to avoid conflict
            let _get_resume_guard = ProfileGuard::new("PIPELINE_sink_get_resumption_state");
            self.sink
                .get_resumption_state()
                .await
                .map_err(PipelineError::Sink)?
        };

        match self.action {
            PipelineAction::TableCopiesOnly => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state_val.copied_tables)
                    .await?;
            }
            PipelineAction::CdcOnly => {
                self.copy_table_schemas().await?;
                self.copy_cdc_events(resumption_state_val.last_lsn).await?;
            }
            PipelineAction::Both => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state_val.copied_tables)
                    .await?;
                self.copy_cdc_events(resumption_state_val.last_lsn).await?;
            }
        }

        Ok(())
    }
}
