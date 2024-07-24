use std::{
    cmp::{max, min, Ordering, Reverse},
    collections::BinaryHeap,
};

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
    NumberLiteral(NumberLiteralNode),
    VectorSelect(VectorSelectNode),
    Sum(SumNode),
    Count(CountNode),
    Average(AverageNode),
    Min(MinNode),
    Max(MaxNode),
    BottomK(BottomKNode),
    TopK(TopKNode),
}

impl ExecutorNode for TNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        match self {
            TNode::VectorSelect(sel) => sel.next_vector(conn),
            TNode::BottomK(sel) => sel.next_vector(conn),
            TNode::TopK(sel) => sel.next_vector(conn),
            _ => panic!("next_vector not implemented for this node"),
        }
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self {
            TNode::NumberLiteral(sel) => sel.next_scalar(conn),
            TNode::Sum(sel) => sel.next_scalar(conn),
            TNode::Count(sel) => sel.next_scalar(conn),
            TNode::Average(sel) => sel.next_scalar(conn),
            TNode::Min(sel) => sel.next_scalar(conn),
            TNode::Max(sel) => sel.next_scalar(conn),
            _ => panic!("next_scalar not implemented for this node"),
        }
    }
}

pub struct NumberLiteralNode {
    val: Value,
}

impl NumberLiteralNode {
    pub fn new(val: f64) -> Self {
        Self { val: val as Value } // TODO: Allow for floats
    }
}

impl ExecutorNode for NumberLiteralNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        Some(self.val)
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

        while let Some((t, v)) = self.child.next_vector(conn) {
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

        while let Some((t, v)) = self.child.next_vector(conn) {
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

        while let Some((t, v)) = self.child.next_vector(conn) {
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

        while let Some((t, v)) = self.child.next_vector(conn) {
            max_val = max(max_val, v);
        }

        Some(max_val)
    }
}

#[derive(Eq)]
struct ValueOrderedVector(Timestamp, Value); // implements Ord to order by Value (rather than Timestamp)

impl PartialEq for ValueOrderedVector {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl Ord for ValueOrderedVector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.cmp(&other.1)
    }
}

impl PartialOrd for ValueOrderedVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct BottomKNode {
    ix: usize,
    bottomk: Vec<(Timestamp, Value)>,
}

impl BottomKNode {
    pub fn new(conn: &mut Connection, mut child: Box<TNode>, mut param: Box<TNode>) -> Self {
        let k = param.next_scalar(conn).unwrap();
        let mut maxheap: BinaryHeap<ValueOrderedVector> = BinaryHeap::new();

        // Find (up to) k smallest values
        // Newer values overwrite older values in case of ties
        if (k > 0) {
            while let Some((t, v)) = child.next_vector(conn) {
                if (maxheap.len() < k.try_into().unwrap()) {
                    maxheap.push(ValueOrderedVector(t, v));
                } else if (v <= maxheap.peek().unwrap().1) {
                    maxheap.pop();
                    maxheap.push(ValueOrderedVector(t, v));
                }
            }
        }

        // Re-sort values by timestamp
        let mut bottomk: Vec<(Timestamp, Value)> =
            maxheap.into_iter().map(|x| (x.0, x.1)).collect();
        bottomk.sort();

        Self { ix: 0, bottomk }
    }
}

impl ExecutorNode for BottomKNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        if (self.ix >= self.bottomk.len()) {
            None
        } else {
            let next = self.bottomk[self.ix];
            self.ix += 1;
            Some(next)
        }
    }
}

pub struct TopKNode {
    ix: usize,
    topk: Vec<(Timestamp, Value)>,
}

impl TopKNode {
    pub fn new(conn: &mut Connection, mut child: Box<TNode>, mut param: Box<TNode>) -> Self {
        let k = param.next_scalar(conn).unwrap();
        let mut minheap: BinaryHeap<Reverse<ValueOrderedVector>> = BinaryHeap::new();

        // Find (up to) k largest values
        // Newer values overwrite older values in case of ties
        if (k > 0) {
            while let Some((t, v)) = child.next_vector(conn) {
                if (minheap.len() < k.try_into().unwrap()) {
                    minheap.push(Reverse(ValueOrderedVector(t, v)));
                } else if (v >= minheap.peek().unwrap().0 .1) {
                    minheap.pop();
                    minheap.push(Reverse(ValueOrderedVector(t, v)));
                }
            }
        }

        // Re-sort values by timestamp
        let mut topk: Vec<(Timestamp, Value)> =
            minheap.into_iter().map(|x| (x.0 .0, x.0 .1)).collect();
        topk.sort();

        Self { ix: 0, topk }
    }
}

impl ExecutorNode for TopKNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        if (self.ix >= self.topk.len()) {
            None
        } else {
            let next = self.topk[self.ix];
            self.ix += 1;
            Some(next)
        }
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
