use std::{
    cmp::{max, min, Ordering, Reverse},
    collections::BinaryHeap,
};

use promql_parser::label::{Matcher, Matchers};
use uuid::Uuid;

use crate::{
    api::{Connection, TachyonResultType},
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

    fn return_type(&self) -> TachyonResultType {
        panic!("Return type not implemented")
    }
}

pub enum TNode {
    NumberLiteral(NumberLiteralNode),
    VectorSelect(VectorSelectNode),
    BinaryOp(BinaryOpNode),
    VectorToVector(VectorToVectorNode),
    VectorToScalar(VectorToScalarNode),
    ScalarToScalar(ScalarToScalarNode),
    Sum(SumNode),
    Count(CountNode),
    Average(AverageNode),
    Min(MinNode),
    Max(MaxNode),
    BottomK(BottomKNode),
    TopK(TopKNode),
}

impl ExecutorNode for TNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self {
            TNode::NumberLiteral(sel) => sel.next_scalar(conn),
            TNode::BinaryOp(sel) => sel.next_scalar(conn),
            TNode::ScalarToScalar(sel) => sel.next_scalar(conn),
            TNode::Sum(sel) => sel.next_scalar(conn),
            TNode::Count(sel) => sel.next_scalar(conn),
            TNode::Average(sel) => sel.next_scalar(conn),
            TNode::Min(sel) => sel.next_scalar(conn),
            TNode::Max(sel) => sel.next_scalar(conn),
            TNode::BottomK(sel) => sel.next_scalar(conn),
            TNode::TopK(sel) => sel.next_scalar(conn),
            _ => panic!("next_scalar not implemented for this node"),
        }
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        match self {
            TNode::VectorSelect(sel) => sel.next_vector(conn),
            TNode::BinaryOp(sel) => sel.next_vector(conn),
            TNode::VectorToVector(sel) => sel.next_vector(conn),
            TNode::VectorToScalar(sel) => sel.next_vector(conn),
            _ => panic!("next_vector not implemented for this node"),
        }
    }

    fn return_type(&self) -> TachyonResultType {
        match self {
            TNode::NumberLiteral(sel) => sel.return_type(),
            TNode::VectorSelect(sel) => sel.return_type(),
            TNode::BinaryOp(sel) => sel.return_type(),
            TNode::VectorToVector(sel) => sel.return_type(),
            TNode::VectorToScalar(sel) => sel.return_type(),
            TNode::ScalarToScalar(sel) => sel.return_type(),
            TNode::Sum(sel) => sel.return_type(),
            TNode::Count(sel) => sel.return_type(),
            TNode::Average(sel) => sel.return_type(),
            TNode::Min(sel) => sel.return_type(),
            TNode::Max(sel) => sel.return_type(),
            TNode::BottomK(sel) => sel.return_type(),
            TNode::TopK(sel) => sel.return_type(),
            _ => panic!("return_type not implemented for this node"),
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

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
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

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Vectors
    }
}

pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

impl BinaryOp {
    pub fn apply(&self, lhs: Value, rhs: Value) -> Value {
        match self {
            BinaryOp::Add => lhs + rhs,
            BinaryOp::Subtract => lhs - rhs,
            BinaryOp::Multiply => lhs * rhs,
            BinaryOp::Divide => lhs / rhs,
            BinaryOp::Modulo => lhs % rhs,
        }
    }
}

pub struct BinaryOpNode {
    child: Box<TNode>,
    return_type_: TachyonResultType,
}

impl BinaryOpNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        match (lhs.return_type(), rhs.return_type()) {
            (TachyonResultType::Scalar, TachyonResultType::Scalar) => {
                let child: Box<TNode> =
                    Box::new(TNode::ScalarToScalar(ScalarToScalarNode::new(op, lhs, rhs)));
                Self {
                    child,
                    return_type_: TachyonResultType::Scalar,
                }
            }

            (TachyonResultType::Vectors, TachyonResultType::Scalar) => {
                let child: Box<TNode> =
                    Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, lhs, rhs)));
                Self {
                    child,
                    return_type_: TachyonResultType::Vectors,
                }
            }

            (TachyonResultType::Scalar, TachyonResultType::Vectors) => {
                let child: Box<TNode> =
                    Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, rhs, lhs)));
                Self {
                    child,
                    return_type_: TachyonResultType::Vectors,
                }
            }

            (TachyonResultType::Vectors, TachyonResultType::Vectors) => {
                let child: Box<TNode> =
                    Box::new(TNode::VectorToVector(VectorToVectorNode::new(op, rhs, lhs)));
                Self {
                    child,
                    return_type_: TachyonResultType::Vectors,
                }
            }

            _ => panic!("VectorBinaryOpNode is not implemented for this return type."),
        }
    }
}

impl ExecutorNode for BinaryOpNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        self.child.next_scalar(conn)
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        self.child.next_vector(conn)
    }

    fn return_type(&self) -> TachyonResultType {
        self.return_type_
    }
}

pub struct ScalarToScalarNode {
    op: BinaryOp,
    lhs: Box<TNode>,
    rhs: Box<TNode>,
}

impl ScalarToScalarNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        Self { op, lhs, rhs }
    }
}

impl ExecutorNode for ScalarToScalarNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let lhs_opt = self.lhs.next_scalar(conn);
        let rhs_opt = self.rhs.next_scalar(conn);

        match (lhs_opt, rhs_opt) {
            (Some(lhs_value), Some(rhs_value)) => Some(self.op.apply(lhs_value, rhs_value)),
            _ => None,
        }
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
    }
}

pub struct VectorToScalarNode {
    op: BinaryOp,
    vector_node: Box<TNode>,
    scalar_node: Box<TNode>,
    scalar: Option<Value>,
}

impl VectorToScalarNode {
    pub fn new(op: BinaryOp, vector_node: Box<TNode>, scalar_node: Box<TNode>) -> Self {
        Self {
            op,
            vector_node,
            scalar_node,
            scalar: None,
        }
    }
}

impl ExecutorNode for VectorToScalarNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        let vector_opt = self.vector_node.next_vector(conn);

        let scalar = match self.scalar {
            Some(s) => s,
            None => {
                self.scalar = self.scalar_node.next_scalar(conn);
                self.scalar.unwrap()
            }
        };

        if let Some((timestamp, value)) = vector_opt {
            Some((timestamp, self.op.apply(value, scalar)))
        } else {
            None
        }
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Vectors
    }
}

pub struct VectorToVectorNode {
    op: BinaryOp,
    lhs: Box<TNode>,
    rhs: Box<TNode>,
}

impl VectorToVectorNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        Self { op, lhs, rhs }
    }
}

impl ExecutorNode for VectorToVectorNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        let lhs_vector = self.lhs.next_vector(conn);
        let rhs_vector = self.rhs.next_vector(conn);

        if lhs_vector.is_none() || rhs_vector.is_none() {
            return None;
        }

        let (lhs_timestamp, lhs_value) = lhs_vector.unwrap();
        let (rhs_timestamp, rhs_value) = rhs_vector.unwrap();

        if lhs_timestamp != rhs_timestamp {
            // TODO: Handle timestamp matching
            todo!("Timestamps don't match!");
        }

        Some((lhs_timestamp, self.op.apply(lhs_value, rhs_value)))
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Vectors
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
        let first_vector = self.child.next_vector(conn);

        first_vector?;

        let mut sum = first_vector.unwrap().1;

        while let Some((t, v)) = self.child.next_vector(conn) {
            sum += v;
        }

        Some(sum)
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
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
        let first_vector = self.child.next_vector(conn);

        first_vector?;

        let mut count = 0;

        if let TNode::VectorSelect(_) = *self.child {
            count += first_vector.unwrap().1;
            while let Some((t, v)) = self.child.next_vector(conn) {
                count += v;
            }
        } else {
            count += 1;
            while self.child.next_vector(conn).is_some() {
                count += 1;
            }
        }

        Some(count)
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
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
        let sum_opt = self.sum.next_scalar(conn);
        let count_opt = self.count.next_scalar(conn);

        match (sum_opt, count_opt) {
            (Some(sum), Some(count)) if count != 0 => Some(sum / count), // TODO: Allow for floats
            _ => None,
        }
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
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

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
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

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
    }
}

pub struct BottomKNode {
    ix: usize,
    bottomk: Vec<Value>,
}

impl BottomKNode {
    pub fn new(conn: &mut Connection, mut child: Box<TNode>, mut param: Box<TNode>) -> Self {
        let k = param.next_scalar(conn).unwrap();
        let mut maxheap: BinaryHeap<Value> = BinaryHeap::new();

        // Find (up to) k smallest values
        // Newer values overwrite older values in case of ties
        if (k > 0) {
            while let Some((t, v)) = child.next_vector(conn) {
                if (maxheap.len() < k.try_into().unwrap()) {
                    maxheap.push(v);
                } else if (v < *maxheap.peek().unwrap()) {
                    maxheap.pop();
                    maxheap.push(v);
                }
            }
        }

        // Re-sort values by timestamp
        let mut bottomk: Vec<Value> = maxheap.into_iter().collect();
        bottomk.sort();

        Self { ix: 0, bottomk }
    }
}

impl ExecutorNode for BottomKNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        if (self.ix >= self.bottomk.len()) {
            None
        } else {
            let next = self.bottomk[self.ix];
            self.ix += 1;
            Some(next)
        }
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalars
    }
}

pub struct TopKNode {
    ix: usize,
    topk: Vec<Value>,
}

impl TopKNode {
    pub fn new(conn: &mut Connection, mut child: Box<TNode>, mut param: Box<TNode>) -> Self {
        let k = param.next_scalar(conn).unwrap();
        let mut minheap: BinaryHeap<Reverse<Value>> = BinaryHeap::new();

        // Find (up to) k largest values
        // Newer values overwrite older values in case of ties
        if (k > 0) {
            while let Some((t, v)) = child.next_vector(conn) {
                if (minheap.len() < k.try_into().unwrap()) {
                    minheap.push(Reverse(v));
                } else if (v >= minheap.peek().unwrap().0) {
                    minheap.pop();
                    minheap.push(Reverse(v));
                }
            }
        }

        // Re-sort values by timestamp
        let mut topk: Vec<Value> = minheap.into_iter().map(|x| x.0).collect();
        topk.sort();
        topk.reverse();

        Self { ix: 0, topk }
    }
}

impl ExecutorNode for TopKNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        if (self.ix >= self.topk.len()) {
            None
        } else {
            let next = self.topk[self.ix];
            self.ix += 1;
            Some(next)
        }
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalars
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
