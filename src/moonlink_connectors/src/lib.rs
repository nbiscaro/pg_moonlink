mod pg_replicate;
mod postgres;

pub use pg_replicate::pipeline::sources::postgres::PostgresSourceError;
pub use postgres::{MoonlinkPostgresSource, PostgresSourceMetadata};
