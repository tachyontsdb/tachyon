use std::{
    cmp::{max, min, Ordering, Reverse},
    collections::{BinaryHeap, VecDeque}
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
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        match self {
            TNode::VectorSelect(sel) => sel.next_vector(conn),
            TNode::VectorToVector(sel) => sel.next_vector(conn),
            TNode::VectorToScalar(sel) => sel.next_vector(conn),
            TNode::BinaryOp(sel) => sel.next_vector(conn),
            TNode::BottomK(sel) => sel.next_vector(conn),
            TNode::TopK(sel) => sel.next_vector(conn),
            _ => panic!("next_vector not implemented for this node"),
        }
    }

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
            _ => panic!("next_scalar not implemented for this node"),
        }
    }

    fn return_type(&self) -> TachyonResultType {
        match self {
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

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Vector
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

            (TachyonResultType::Vector, TachyonResultType::Scalar) => {
                let child: Box<TNode> =
                    Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, lhs, rhs)));
                Self {
                    child,
                    return_type_: TachyonResultType::Vector,
                }
            }

            (TachyonResultType::Scalar, TachyonResultType::Vector) => {
                let child: Box<TNode> =
                    Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, rhs, lhs)));
                Self {
                    child,
                    return_type_: TachyonResultType::Vector,
                }
            }

            (TachyonResultType::Vector, TachyonResultType::Vector) => {
                let child: Box<TNode> =
                    Box::new(TNode::VectorToVector(VectorToVectorNode::new(op, rhs, lhs)));
                Self {
                    child,
                    return_type_: TachyonResultType::Vector,
                }
            }

            _ => panic!("VectorBinaryOpNode is not implemented for this return type."),
        }
    }
}

impl ExecutorNode for BinaryOpNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        self.child.next_vector(conn)
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        self.child.next_scalar(conn)
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
        TachyonResultType::Scalar
    }
}

pub struct VectorToVectorNode {
    op: BinaryOp,
    lhs: Box<TNode>,
    rhs: Box<TNode>,
    lhs_range: VecDeque<(Timestamp, Value)>,
    rhs_range: VecDeque<(Timestamp, Value)>,
    value_opt: Option<(Timestamp, Value, VectorToVectorStream)>,
}

#[derive(Clone, Copy)]
enum VectorToVectorStream {
    Lhs = 0,
    Rhs = 1,
}

impl VectorToVectorNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        Self {
            op,
            lhs,
            rhs,
            lhs_range: VecDeque::new(),
            rhs_range: VecDeque::new(),
            value_opt: None,
        }
    }

    fn calculate_value_with_linear_interpolation(
        &self,
        ts: Timestamp,
        stream: VectorToVectorStream,
    ) -> Value {
        let range = match stream {
            VectorToVectorStream::Lhs => &self.lhs_range,
            VectorToVectorStream::Rhs => &self.rhs_range,
        };

        if range.len() == 1 {
            range[0].1
        } else if range.len() == 2 {
            let (v1, v2) = (range[1].1, range[0].1);
            let (t1, t2) = (range[1].0, range[0].0);

            let slope = (v2 as f64 - v1 as f64) / (t2 as f64 - t1 as f64);
            ((ts as f64 - t1 as f64) * slope + v1 as f64).round() as Value
        } else {
            panic!("No values in range for interpolation.")
        }
    }

    fn next_child_vector(
        &mut self,
        conn: &mut Connection,
        stream: VectorToVectorStream,
    ) -> Option<(Timestamp, Value)> {
        let (node, range) = match stream {
            VectorToVectorStream::Lhs => (&mut self.lhs, &mut self.lhs_range),
            VectorToVectorStream::Rhs => (&mut self.rhs, &mut self.rhs_range),
        };

        let vec_opt = node.next_vector(conn);
        if let Some(vec) = vec_opt {
            range.push_front(vec);

            if range.len() > 2 {
                range.pop_back();
            }
        }

        vec_opt
    }
}

impl ExecutorNode for VectorToVectorNode {
    // Initial case
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        if self.lhs_range.is_empty() && self.rhs_range.is_empty() && self.value_opt.is_none() {
            let lhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs);
            let rhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs);

            match (lhs_vector_opt, rhs_vector_opt) {
                (Some((lhs_ts, lhs_val)), Some((rhs_ts, rhs_val))) => {
                    match lhs_ts.cmp(&rhs_ts) {
                        std::cmp::Ordering::Less => {
                            // Store this value
                            self.value_opt = Some((rhs_ts, rhs_val, VectorToVectorStream::Rhs));
                        }
                        std::cmp::Ordering::Greater => {
                            // Store this value
                            self.value_opt = Some((lhs_ts, lhs_val, VectorToVectorStream::Lhs));
                        }
                        std::cmp::Ordering::Equal => {}
                    }

                    return Some((
                        Timestamp::min(lhs_ts, rhs_ts),
                        self.op.apply(lhs_val, rhs_val), // These are the first values of the stream thus no interpolation is necessary
                    ));
                }
                _ => return None, // One of the streams is empty. Then do nothing.
            };
        } else if self.value_opt.is_some() {
            // There is a residual value from one stream
            let (value_ts, value_val, last_stream) = self.value_opt.unwrap();
            match last_stream {
                VectorToVectorStream::Lhs => {
                    let rhs_vec_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs); // Fetch value from opposing stream
                    match rhs_vec_opt {
                        Some((rhs_ts, rhs_val)) => {
                            match rhs_ts.cmp(&value_ts) {
                                // If the new vector is less than the residual we must interpolate the residual value relative to the new vector
                                std::cmp::Ordering::Less => {
                                    let lhs_interpolated = self
                                        .calculate_value_with_linear_interpolation(
                                            rhs_ts,
                                            VectorToVectorStream::Lhs,
                                        );
                                    return Some((
                                        rhs_ts,
                                        self.op.apply(lhs_interpolated, rhs_val),
                                    ));
                                }
                                // If the new vector is greater than the residual we must store it, and then interpolate it relative to the residual
                                std::cmp::Ordering::Greater => {
                                    self.value_opt =
                                        Some((rhs_ts, rhs_val, VectorToVectorStream::Rhs));
                                    let rhs_interpolated = self
                                        .calculate_value_with_linear_interpolation(
                                            value_ts,
                                            VectorToVectorStream::Rhs,
                                        );
                                    return Some((
                                        value_ts,
                                        self.op.apply(value_val, rhs_interpolated),
                                    ));
                                }
                                // If they are equal then no interpolation needed, we can discard residual value
                                std::cmp::Ordering::Equal => {
                                    self.value_opt = None;
                                    return Some((value_ts, self.op.apply(value_val, rhs_val)));
                                }
                            }
                        }
                        _ => {
                            let rhs_interpolated = self.rhs_range[0].1; // If there is no value from the RHS stream, then we must interpolate based on the last value
                            self.value_opt = None;
                            return Some((value_ts, self.op.apply(value_val, rhs_interpolated)));
                        }
                    }
                }
                VectorToVectorStream::Rhs => {
                    let lhs_vec_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs); // Fetch value from opposing stream
                    match lhs_vec_opt {
                        Some((lhs_ts, lhs_val)) => match lhs_ts.cmp(&value_ts) {
                            // If the new vector is less than the residual we must interpolate the residual value relative to the new vector
                            std::cmp::Ordering::Less => {
                                let rhs_interpolated = self
                                    .calculate_value_with_linear_interpolation(
                                        lhs_ts,
                                        VectorToVectorStream::Rhs,
                                    );
                                return Some((lhs_ts, self.op.apply(lhs_val, rhs_interpolated)));
                            }
                            // If the new vector is greater than the residual we must store it, and then interpolate it relative to the residual
                            std::cmp::Ordering::Greater => {
                                self.value_opt = Some((lhs_ts, lhs_val, VectorToVectorStream::Lhs));
                                let lhs_interpolated = self
                                    .calculate_value_with_linear_interpolation(
                                        value_ts,
                                        VectorToVectorStream::Lhs,
                                    );
                                return Some((
                                    value_ts,
                                    self.op.apply(lhs_interpolated, value_val),
                                ));
                            }
                            // If they are equal then no interpolation needed, we can discard residual value
                            std::cmp::Ordering::Equal => {
                                self.value_opt = None;
                                return Some((value_ts, self.op.apply(lhs_val, value_val)));
                            }
                        },
                        _ => {
                            let lhs_interpolated = self.lhs_range[0].1; // If there is no value from the LHS stream, then we must interpolate based on the last value
                            self.value_opt = None;
                            return Some((value_ts, self.op.apply(lhs_interpolated, value_val)));
                        }
                    }
                }
            }
        } else {
            // There is no residual value present
            let lhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs);
            let rhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs);

            match (lhs_vector_opt, rhs_vector_opt) {
                (Some((lhs_ts, lhs_val)), Some((rhs_ts, rhs_val))) => match lhs_ts.cmp(&rhs_ts) {
                    // If the LHS is less than the RHS we must store the RHS and interpolate it relative to the LHS
                    std::cmp::Ordering::Less => {
                        self.value_opt = Some((rhs_ts, rhs_val, VectorToVectorStream::Rhs));
                        let rhs_interpolated = self.calculate_value_with_linear_interpolation(
                            lhs_ts,
                            VectorToVectorStream::Rhs,
                        );
                        return Some((lhs_ts, self.op.apply(lhs_val, rhs_interpolated)));
                    }
                    // If the LHS is greater than the RHS we must store the LHS and interpolate it relative to the RHS
                    std::cmp::Ordering::Greater => {
                        self.value_opt = Some((lhs_ts, lhs_val, VectorToVectorStream::Lhs));
                        let lhs_interpolated = self.calculate_value_with_linear_interpolation(
                            rhs_ts,
                            VectorToVectorStream::Lhs,
                        );
                        return Some((rhs_ts, self.op.apply(lhs_interpolated, rhs_val)));
                    }
                    // If equal then no interpolation necessary
                    std::cmp::Ordering::Equal => {
                        return Some((lhs_ts, self.op.apply(lhs_val, rhs_val)));
                    }
                },
                // If no RHS value then we interpolate it based on the last one
                (Some((lhs_ts, lhs_val)), None) => {
                    let rhs_interpolated = self.rhs_range[0].1;
                    return Some((lhs_ts, self.op.apply(lhs_val, rhs_interpolated)));
                }
                // If no LHS value then we interpolate it based on the last one
                (None, Some((rhs_ts, rhs_val))) => {
                    let lhs_interpolated = self.lhs_range[0].1;
                    return Some((rhs_ts, self.op.apply(lhs_interpolated, rhs_val)));
                }
                _ => return None, // Both of the streams are empty.
            };
        }

        panic!("VectorToVector interpolation hit impossible case!")
    }

    fn return_type(&self) -> TachyonResultType {
        TachyonResultType::Scalar
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
