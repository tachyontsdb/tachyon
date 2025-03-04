use super::ExecutorNode;
use crate::error::QueryErr;
use crate::query::indexer::Indexer;
use crate::storage::file::{Cursor, ScanHint};
use crate::storage::page_cache::PageCache;
use crate::{Connection, ReturnType, Timestamp, ValueType, Vector};
use promql_parser::label::Matchers;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;

pub struct VectorSelectNode {
    stream_ids: Vec<Uuid>,
    stream_idx: usize,
    cursor: Cursor,
    indexer: Rc<RefCell<Indexer>>,
    page_cache: Rc<RefCell<PageCache>>,
    start: Timestamp,
    end: Timestamp,
    hint: ScanHint,
}

impl VectorSelectNode {
    pub fn new(
        conn: &mut Connection,
        name: String,
        matchers: Matchers,
        start: Timestamp,
        end: Timestamp,
        hint: ScanHint,
    ) -> Result<Self, QueryErr> {
        let stream_ids: Vec<Uuid> = conn
            .indexer
            .borrow()
            .get_stream_ids(&name, &matchers)
            .into_iter()
            .collect();

        if stream_ids.is_empty() {
            return Err(QueryErr::NoStreamsMatchedErr {
                name,
                matchers,
                start,
                end,
            });
        }

        let stream_id = stream_ids[0];
        // TODO: get rid of unwrap
        let file_paths = conn
            .indexer
            .borrow()
            .get_required_files(stream_id, start, end)
            .unwrap();

        Ok(Self {
            stream_ids,
            stream_idx: 0,
            cursor: Cursor::new(file_paths, start, end, conn.page_cache.clone(), hint).unwrap(),
            indexer: conn.indexer.clone(),
            page_cache: conn.page_cache.clone(),
            start,
            end,
            hint,
        })
    }
}

impl ExecutorNode for VectorSelectNode {
    fn value_type(&self) -> ValueType {
        self.cursor.value_type()
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Vector
    }

    fn next_vector(&mut self, _: &mut Connection) -> Option<Vector> {
        if self.cursor.is_done() {
            self.stream_idx += 1;
            if self.stream_idx >= self.stream_ids.len() {
                return None;
            }

            let stream_id = self.stream_ids[self.stream_idx];
            // TODO: get rid of unwrap
            let file_paths = self
                .indexer
                .borrow()
                .get_required_files(stream_id, self.start, self.end)
                .unwrap();

            self.cursor = Cursor::new(
                file_paths,
                self.start,
                self.end,
                self.page_cache.clone(),
                self.hint,
            )
            .unwrap();
        }
        let res = self.cursor.fetch();
        self.cursor.next();
        Some(res)
    }
}
