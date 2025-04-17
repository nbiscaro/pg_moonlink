// Corresponds to the Parquet Types
#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal(i128),
    Bool(bool),
    ByteArray(Vec<u8>),
    FixedLenByteArray([u8; 16]), // uuid & certain numeric
    Null,
}
