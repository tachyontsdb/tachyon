use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use crate::common::{Timestamp, Value};
use crate::executor::node::{ExecutorNode, TNode};
use crate::query::indexer::Indexer;
use crate::query::planner::QueryPlanner;
use crate::storage::page_cache::PageCache;
use crate::storage::writer::Writer;
use promql_parser::parser;
use uuid::Uuid;

#[repr(C)]
pub struct Connection {
    pub root_dir: PathBuf,
    pub indexer: Rc<RefCell<Indexer>>,
    pub writer: Writer,
    pub page_cache: Rc<RefCell<PageCache>>,
}

impl Connection {
    pub fn new(root_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&root_dir).unwrap();
        let mut indexer = Rc::new(RefCell::new(Indexer::new(root_dir.clone())));
        indexer.borrow_mut().create_store();

        Self {
            root_dir: root_dir.clone(),
            indexer: indexer.clone(),
            writer: Writer::new(root_dir, indexer),
            page_cache: Rc::new(RefCell::new(PageCache::new(10))),
        }
    }

    pub fn prepare(&mut self, s: &str, start: Option<Timestamp>, end: Option<Timestamp>) -> Stmt {
        let ast = parser::parse(s).unwrap();
        let mut planner = QueryPlanner::new(&ast, start, end);
        let plan = planner.plan(self);

        Stmt {
            root: plan,
            connection: self,
        }
    }

    pub fn insert(&mut self, s: &str, timestamp: Timestamp, value: Value) {
        let id = self.get_stream_id_from_matcher(s);

        self.writer.write(id, timestamp, value);
    }

    pub fn batch_insert(&mut self, s: &str) -> BatchWriter {
        let stream_id = self.get_stream_id_from_matcher(s);

        BatchWriter {
            stream_id,
            writer: &mut self.writer,
        }
    }

    fn get_stream_id_from_matcher(&mut self, s: &str) -> Uuid {
        let ast = parser::parse(s).unwrap();

        let vec_sel = match ast {
            parser::Expr::VectorSelector(vec_sel) => vec_sel,
            _ => panic!("Expected a vector selector!"),
        };

        if vec_sel.at.is_some() || vec_sel.offset.is_some() {
            panic!("Cannot include at / offset for insert query");
        }

        let stream_ids = self
            .indexer
            .borrow()
            .get_stream_ids(vec_sel.name.as_ref().unwrap(), &vec_sel.matchers);

        if stream_ids.len() > 1 {
            panic!("Multiple streams matched selector. Can only insert into one stream at a time.");
        }

        if stream_ids.is_empty() {
            let stream_id = self
                .indexer
                .borrow_mut()
                .insert_new_id(vec_sel.name.as_ref().unwrap(), &vec_sel.matchers);
            self.writer.create_stream(stream_id);
            stream_id
        } else {
            stream_ids.into_iter().next().unwrap()
        }
    }
}

pub struct BatchWriter {
    stream_id: Uuid,
    writer: *mut Writer,
}

impl BatchWriter {
    pub fn insert(&mut self, timestamp: Timestamp, value: Value) {
        unsafe {
            (*self.writer).write(self.stream_id, timestamp, value);
        }
    }
}

#[repr(C)]
pub struct Stmt {
    pub root: TNode,
    connection: *mut Connection, // should be a reference, but has to be FFI-safe
}

impl Stmt {
    pub fn next_scalar(&mut self) -> Option<Value> {
        unsafe { self.root.next_scalar(&mut *self.connection) }
    }

    pub fn next_vector(&mut self) -> Option<(Timestamp, Value)> {
        unsafe { self.root.next_vector(&mut *self.connection) }
    }

    pub fn return_type(&self) -> TachyonResultType {
        match self.root {
            TNode::VectorSelect(_) => TachyonResultType::Vector,
            TNode::Average(_) => TachyonResultType::Scalar,
            TNode::Sum(_) => TachyonResultType::Scalar,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct VectorResult {
    pub timestamp: Timestamp,
    pub value: Value,
}

#[repr(C)]
pub union TachyonResultUnion {
    pub scalar: Value,
    pub vector: VectorResult,
}

#[repr(u8)]
pub enum TachyonResultType {
    Done,
    Scalar,
    Vector,
}

#[repr(C)]
pub struct TachyonResult {
    pub t: TachyonResultType,
    pub r: TachyonResultUnion,
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

    use crate::{api::Connection, utils::test_utils::set_up_dirs};

    #[test]
    fn test_e2e() {
        set_up_dirs!(dirs, "db");

        let root_dir = dirs[0].clone();
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];

        for (t, v) in zip(timestamps, values) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        conn.writer.flush_all();

        let mut stmt = conn.prepare(
            r#"http_requests_total{service = "web"}"#,
            Some(23),
            Some(51),
        );

        let mut i = 0;

        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            assert_eq!(timestamps[i], res.0);
            assert_eq!(values[i], res.1);
            i += 1;
        }

        assert_eq!(i, 4);
    }

    #[test]
    fn test_e2e_multiple_streams() {
        set_up_dirs!(dirs, "db");

        let root_dir = dirs[0].clone();
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];

        for (t, v) in zip(timestamps, values) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        let timestamps_2 = [12, 15, 30, 67];
        let values_2 = [1, 5, 40, 20];

        for (t, v) in zip(timestamps_2, values_2) {
            conn.insert(r#"http_requests_total{service = "cool"}"#, t, v);
        }

        conn.writer.flush_all();

        let mut stmt = conn.prepare(
            r#"http_requests_total{service = "web"}"#,
            Some(23),
            Some(51),
        );

        let mut i = 0;

        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            assert_eq!(timestamps[i], res.0);
            assert_eq!(values[i], res.1);
            i += 1;
        }

        assert_eq!(i, 4);

        let mut stmt = conn.prepare(
            r#"http_requests_total{service = "cool"}"#,
            Some(12),
            Some(67),
        );

        let mut i = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            assert_eq!(timestamps_2[i], res.0);
            assert_eq!(values_2[i], res.1);
            i += 1;
        }

        assert_eq!(i, 4);
    }

    #[test]
    fn test_e2e_aggregation() {
        set_up_dirs!(dirs, "db");

        let root_dir = dirs[0].clone();
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];
        let mut expected_sum = 0;

        for (t, v) in zip(timestamps, values) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
            expected_sum += v;
        }

        conn.writer.flush_all();

        let mut stmt = conn.prepare(
            r#"sum(http_requests_total{service = "web"})"#,
            Some(23),
            Some(51),
        );

        let actual_sum = stmt.next_scalar().unwrap();
        assert_eq!(actual_sum, expected_sum);
    }

    #[test]
    fn test_e2e_aggregation_2() {
        set_up_dirs!(dirs, "db");

        let root_dir = dirs[0].clone();
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 24, 48];
        let mut expected_avg = 0;

        for (t, v) in zip(timestamps, values) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
            expected_avg += v;
        }
        expected_avg /= values.len() as u64;

        conn.writer.flush_all();

        let mut stmt = conn.prepare(
            r#"avg(http_requests_total{service = "web"})"#,
            Some(23),
            Some(51),
        );

        let actual_avg = stmt.next_scalar().unwrap();
        assert_eq!(actual_avg, expected_avg);
    }
}
