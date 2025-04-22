use super::moonlink_type::RowValue;

#[derive(Debug)]
pub struct MoonlinkRow {
    pub values: Vec<RowValue>,
}

impl MoonlinkRow {
    pub fn new(values: Vec<RowValue>) -> Self {
        Self { values }
    }
}
