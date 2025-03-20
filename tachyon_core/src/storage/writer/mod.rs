use std::{cell::RefCell, path::Path, rc::Rc};

use uuid::Uuid;

use crate::{error::WriterErr, query::indexer::Indexer, Timestamp, Value, ValueType, Version};

pub mod in_memory_writer;
pub mod persistent_writer;

pub trait Writer {
    fn new(root: impl AsRef<Path>, indexer: Rc<RefCell<Indexer>>, version: Version) -> Self
    where
        Self: Sized;
    fn write(
        &mut self,
        stream_id: Uuid,
        ts: Timestamp,
        v: Value,
        value_type: ValueType,
    ) -> Result<(), WriterErr>;
    fn flush_all(&mut self) -> Result<(), WriterErr>;
    fn create_stream(&self, stream_id: Uuid) -> Result<(), WriterErr>;
}
