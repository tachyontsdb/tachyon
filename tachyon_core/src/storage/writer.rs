use super::file::TimeDataFile;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use uuid::Uuid;

pub struct Writer {
    open_data_files: HashMap<Uuid, TimeDataFile>, // Stream ID to in-mem file
    root: PathBuf,
    indexer: Rc<RefCell<Indexer>>,
}
