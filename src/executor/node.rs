use std::cmp::{max, min};

use promql_parser::label::{Matcher, Matchers};
use uuid::Uuid;

use crate::{
    api::Connection,
    common::{Timestamp, Value},
    storage::file::{Cursor, ScanHint},
    utils::common::static_assert,
};

pub trait ExecutorNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        panic!("Next scalar not implemented")
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        panic!("Next vector not implemented")
    }
}

pub enum TNode {
    VectorSelect(VectorSelectNode),
    Sum(SumNode),
    Count(CountNode),
    Average(AverageNode),
    Min(MinNode),
    Max(MaxNode),
}

impl ExecutorNode for TNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        match self {
            TNode::VectorSelect(sel) => sel.next_vector(conn),
            _ => panic!("next_vector not implemented for this node"),
        }
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self {
            TNode::Sum(sel) => sel.next_scalar(conn),
            TNode::Count(sel) => sel.next_scalar(conn),
            TNode::Average(sel) => sel.next_scalar(conn),
            TNode::Min(sel) => sel.next_scalar(conn),
            TNode::Max(sel) => sel.next_scalar(conn),
            _ => panic!("next_scalar not implemented for this node"),
        }
    }
}

pub struct VectorSelectNode {
    stream_ids: Vec<Uuid>,
    stream_idx: usize,
    cursor: Cursor,
}

impl VectorSelectNode {
    pub fn new(
        conn: &mut Connection,
        name: String,
        matchers: Matchers,
        start: Timestamp,
        end: Timestamp,
        hint: ScanHint,
    ) -> Self {
        let stream_ids: Vec<Uuid> = conn
            .indexer
            .borrow()
            .get_stream_ids(&name, &matchers)
            .into_iter()
            .collect();

        if stream_ids.is_empty() {
            panic!("No streams match selector");
        }

        let stream_id = stream_ids[0];
        let file_paths = conn
            .indexer
            .borrow()
            .get_required_files(&stream_id, &start, &end);

        VectorSelectNode {
            stream_ids,
            stream_idx: 0,
            cursor: Cursor::new(file_paths, start, end, conn.page_cache.clone(), hint).unwrap(),
        }
    }
}

impl ExecutorNode for VectorSelectNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        if self.cursor.is_done() {
            self.stream_idx += 1;
            // support multiple streams here
            return None;
        }
        let res = self.cursor.fetch();
        self.cursor.next();
        Some(res)
    }
}

pub struct SumNode {
    child: Box<TNode>,
}

impl SumNode {
    pub fn new(child: Box<TNode>) -> Self {
        Self { child }
    }
}

impl ExecutorNode for SumNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let mut sum = 0;

        loop {
            let pair = self.child.next_vector(conn);

            if (pair.is_none()) {
                break;
            }
            let (t, v) = pair.unwrap();
            sum += v;
        }

        Some(sum)
    }
}

pub struct CountNode {
    child: Box<TNode>,
}

impl CountNode {
    pub fn new(child: Box<TNode>) -> Self {
        Self { child }
    }
}

impl ExecutorNode for CountNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let mut count = 0;

        loop {
            let pair = self.child.next_vector(conn);

            if (pair.is_none()) {
                break;
            }
            let (t, v) = pair.unwrap();
            count += v;
        }

        Some(count)
    }
}

pub struct AverageNode {
    sum: Box<SumNode>,
    count: Box<CountNode>,
}

impl AverageNode {
    pub fn new(sum: Box<SumNode>, count: Box<CountNode>) -> Self {
        Self { sum, count }
    }
}

impl ExecutorNode for AverageNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let mut total = 0;
        let sum = self.sum.next_scalar(conn).unwrap();
        let count = self.count.next_scalar(conn).unwrap();

        if (count == 0) {
            None
        } else {
            Some(sum / count) // TODO: Allow for floats
        }
    }
}

pub struct MinNode {
    child: Box<TNode>,
}

impl MinNode {
    pub fn new(child: Box<TNode>) -> Self {
        Self { child }
    }
}

impl ExecutorNode for MinNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let mut is_first_value = true;
        let mut min_val = 0;

        loop {
            let pair = self.child.next_vector(conn);

            if (pair.is_none()) {
                break;
            }

            let (t, v) = pair.unwrap();

            if (is_first_value) {
                min_val = v;
                is_first_value = false;
            }

            min_val = min(min_val, v);
        }

        Some(min_val)
    }
}

pub struct MaxNode {
    child: Box<TNode>,
}

impl MaxNode {
    pub fn new(child: Box<TNode>) -> Self {
        Self { child }
    }
}

impl ExecutorNode for MaxNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let mut max_val = 0;

        loop {
            let pair = self.child.next_vector(conn);

            if (pair.is_none()) {
                break;
            }
            let (t, v) = pair.unwrap();
            max_val = max(max_val, v);
        }

        Some(max_val)
    }
}

mod test {
    use promql_parser::parser;

    #[test]
    fn example_query() {
        let stmt = r#"sum(http_requests_total)"#;
        let ast = parser::parse(stmt);
        println!("{:#?}", ast);
    }
}
