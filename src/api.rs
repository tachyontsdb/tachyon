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

    pub fn insert(&mut self, stream: &str, timestamp: Timestamp, value: Value) {
        let id = self.get_stream_id_from_matcher(stream);
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
    root: TNode,
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
        self.root.return_type()
    }
}

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
#[repr(u8)]
pub enum TachyonResultType {
    Done,
    Scalar,
    Vector,
}

#[cfg(test)]
mod tests {
    use std::{iter::zip, path::PathBuf};

    use crate::{
        api::Connection,
        common::{Timestamp, Value},
        utils::test_utils::set_up_dirs,
    };

    fn e2e_vector_test(
        root_dir: PathBuf,
        start: u64,
        end: u64,
        first_i: usize,
        expected_count: usize,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        conn.writer.flush_all();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"}"#;
        let mut stmt = conn.prepare(query, Some(start), Some(end));

        // Process results
        let mut i = first_i;
        let mut count = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.0);
            assert_eq!(values[i], res.1);
            i += 1;

            count += 1;
        }

        assert_eq!(count, expected_count);
    }

    #[test]
    fn test_e2e_vector_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_test(root_dir, 23, 51, 0, 4);
    }

    #[test]
    fn test_e2e_vector_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_test(root_dir, 29, 40, 1, 2);
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

    fn e2e_scalar_aggregate_test(
        root_dir: PathBuf,
        operation: &str,
        start: u64,
        end: u64,
        expected_val: Value,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        conn.writer.flush_all();

        // Prepare test query
        let query = format!(r#"{}(http_requests_total{{service = "web"}})"#, operation);
        let mut stmt = conn.prepare(&query, Some(start), Some(end));

        // Process results
        let actual_val = stmt.next_scalar().unwrap();
        assert_eq!(actual_val, expected_val);
    }

    #[test]
    fn test_e2e_sum_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 23, 51, 163)
    }

    #[test]
    fn test_e2e_sum_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 29, 40, 70)
    }

    #[test]
    fn test_e2e_count_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 23, 51, 4)
    }

    #[test]
    fn test_e2e_count_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 29, 40, 2)
    }

    #[test]
    fn test_e2e_avg_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 23, 51, 40)
    }

    #[test]
    fn test_e2e_avg_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 29, 40, 35)
    }

    #[test]
    fn test_e2e_min_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 23, 51, 23)
    }

    #[test]
    fn test_e2e_min_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 29, 40, 23)
    }

    #[test]
    fn test_e2e_max_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 23, 51, 48)
    }

    #[test]
    fn test_e2e_max_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 29, 40, 47)
    }

    fn e2e_vector_aggregate_test(
        root_dir: PathBuf,
        operation: &str,
        param: u64,
        start: u64,
        end: u64,
        expected_val: Vec<(Timestamp, Value)>,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 25, 29, 40, 44, 51];
        let values = [27, 31, 47, 23, 31, 48];

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        conn.writer.flush_all();

        // Prepare test query
        let query = format!(
            r#"{}({}, http_requests_total{{service = "web"}})"#,
            operation, param
        );
        let mut stmt = conn.prepare(&query, Some(start), Some(end));

        // Process results
        let mut actual_val: Vec<(Timestamp, Value)> = Vec::new();
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            actual_val.push(res);
        }

        assert_eq!(actual_val, expected_val);
    }

    #[test]
    fn test_e2e_bottomk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(
            root_dir,
            "bottomk",
            2,
            23,
            51,
            [(23, 27), (40, 23)].to_vec(),
        )
    }

    #[test]
    fn test_e2e_bottomk_zero_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(root_dir, "bottomk", 0, 23, 51, [].to_vec())
    }

    #[test]
    fn test_e2e_bottomk_large_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(
            root_dir,
            "bottomk",
            10000,
            23,
            51,
            [(23, 27), (25, 31), (29, 47), (40, 23), (44, 31), (51, 48)].to_vec(),
        )
    }

    #[test]
    fn test_e2e_bottomk_tied_value() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(
            root_dir,
            "bottomk",
            3,
            23,
            51,
            [(23, 27), (40, 23), (44, 31)].to_vec(),
        )
    }

    #[test]
    fn test_e2e_topk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(root_dir, "topk", 2, 23, 51, [(29, 47), (51, 48)].to_vec())
    }

    #[test]
    fn test_e2e_topk_zero_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(root_dir, "topk", 0, 23, 51, [].to_vec())
    }

    #[test]
    fn test_e2e_topk_large_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(
            root_dir,
            "topk",
            10000,
            23,
            51,
            [(23, 27), (25, 31), (29, 47), (40, 23), (44, 31), (51, 48)].to_vec(),
        )
    }

    #[test]
    fn test_e2e_topk_tied_value() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_aggregate_test(
            root_dir,
            "topk",
            3,
            23,
            51,
            [(29, 47), (44, 31), (51, 48)].to_vec(),
        )
    }

    #[test]
    fn test_vector_to_vector_no_interpolation() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        for (t, v) in zip(timestamps, values_b) {
            conn.insert(r#"http_requests_total{service = "mobile"}"#, t, v);
        }

        conn.writer.flush_all();

        // Prepare test query
        let query =
            r#"http_requests_total{service = "web"} * http_requests_total{service = "mobile"}"#;
        let mut stmt = conn.prepare(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        let mut count = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.0);
            assert_eq!(values_a[i] * values_b[i], res.1);
            i += 1;
        }
    }

    #[test]
    fn test_vector_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        for (t, v) in zip(timestamps, values_b) {
            conn.insert(r#"http_requests_total{service = "mobile"}"#, t, v);
        }

        conn.writer.flush_all();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"} + sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        let mut count = 0;
        let sum_values_b = values_b.iter().sum::<Value>();
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.0);
            assert_eq!(values_a[i] + sum_values_b, res.1);
            i += 1;
        }
    }

    #[test]
    fn test_scalar_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            conn.insert(r#"http_requests_total{service = "web"}"#, t, v);
        }

        for (t, v) in zip(timestamps, values_b) {
            conn.insert(r#"http_requests_total{service = "mobile"}"#, t, v);
        }

        conn.writer.flush_all();

        // Prepare test query
        let query = r#"sum(http_requests_total{service = "web"}) / sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        let mut count = 0;
        let sum_values_a = values_a.iter().sum::<Value>();
        let sum_values_b = values_b.iter().sum::<Value>();

        loop {
            let res = stmt.next_scalar();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(sum_values_a / sum_values_b, res);
            i += 1;
        }
    }
}
