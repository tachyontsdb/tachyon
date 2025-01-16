use std::{cell::RefCell, path::Path, rc::Rc};

use uuid::Uuid;

use crate::{query::indexer::Indexer, Timestamp, Value, ValueType, Version};

pub mod writer;
pub mod persistent_writer;

pub trait Writer {
    fn new(root: impl AsRef<Path>, indexer: Rc<RefCell<Indexer>>, version: Version) -> Self
    where
        Self: Sized;
    fn write(&mut self, stream_id: Uuid, ts: Timestamp, v: Value, value_type: ValueType);
    fn flush_all(&mut self);
    fn create_stream(&self, stream_id: Uuid);
}