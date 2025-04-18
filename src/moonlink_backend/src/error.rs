use moonlink_connectors::pg_replicate::pipeline::sources::postgres::PostgresSourceError;
use std::result;
use thiserror::Error;

/// Custom error type for moonlink_backend
#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres source error: {0}")]
    PostgresSource(#[from] PostgresSourceError),
}

pub type Result<T> = result::Result<T, Error>;
