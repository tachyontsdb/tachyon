use super::indexer::Indexer;
use crate::storage::file::{Cursor, ScanHint};
use crate::storage::page_cache::PageCache;
use crate::{Connection, ReturnType, Timestamp, Value, ValueType, Vector};
use promql_parser::label::Matchers;
use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::rc::Rc;
use uuid::Uuid;

pub trait ExecutorNode {
    fn value_type(&self) -> ValueType {
        panic!("Value type not implemented!");
    }

    fn return_type(&self) -> ReturnType {
        panic!("Return type not implemented!");
    }

    fn next_scalar(&mut self, _connection: &mut Connection) -> Option<Value> {
        panic!("Next scalar not implemented!");
    }

    fn next_vector(&mut self, _connection: &mut Connection) -> Option<Vector> {
        panic!("Next vector not implemented!");
    }
}

#[allow(clippy::large_enum_variant)]
pub enum TNode {
    NumberLiteral(NumberLiteralNode),
    VectorSelect(VectorSelectNode),
    BinaryOp(BinaryOpNode),
    VectorToVector(VectorToVectorNode),
    VectorToScalar(VectorToScalarNode),
    ScalarToScalar(ScalarToScalarNode),
    Aggregate(AggregateNode),
    Average(AverageNode),
    GetK(GetKNode),
}

impl ExecutorNode for TNode {
    fn value_type(&self) -> ValueType {
        match self {
            TNode::NumberLiteral(sel) => sel.value_type(),
            TNode::VectorSelect(sel) => sel.value_type(),
            TNode::BinaryOp(sel) => sel.value_type(),
            TNode::VectorToVector(sel) => sel.value_type(),
            TNode::VectorToScalar(sel) => sel.value_type(),
            TNode::ScalarToScalar(sel) => sel.value_type(),
            TNode::Aggregate(sel) => sel.value_type(),
            TNode::Average(sel) => sel.value_type(),
            TNode::GetK(sel) => sel.value_type(),
        }
    }

    fn return_type(&self) -> ReturnType {
        match self {
            TNode::NumberLiteral(sel) => sel.return_type(),
            TNode::VectorSelect(sel) => sel.return_type(),
            TNode::BinaryOp(sel) => sel.return_type(),
            TNode::VectorToVector(sel) => sel.return_type(),
            TNode::VectorToScalar(sel) => sel.return_type(),
            TNode::ScalarToScalar(sel) => sel.return_type(),
            TNode::Aggregate(sel) => sel.return_type(),
            TNode::Average(sel) => sel.return_type(),
            TNode::GetK(sel) => sel.return_type(),
        }
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self {
            TNode::NumberLiteral(sel) => sel.next_scalar(conn),
            TNode::BinaryOp(sel) => sel.next_scalar(conn),
            TNode::ScalarToScalar(sel) => sel.next_scalar(conn),
            TNode::Aggregate(sel) => sel.next_scalar(conn),
            TNode::Average(sel) => sel.next_scalar(conn),
            TNode::GetK(sel) => sel.next_scalar(conn),
            _ => panic!("next_scalar not implemented for this node!"),
        }
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        match self {
            TNode::VectorSelect(sel) => sel.next_vector(conn),
            TNode::VectorToVector(sel) => sel.next_vector(conn),
            TNode::VectorToScalar(sel) => sel.next_vector(conn),
            TNode::BinaryOp(sel) => sel.next_vector(conn),
            _ => panic!("next_vector not implemented for this node!"),
        }
    }
}

pub struct NumberLiteralNode {
    val_type: ValueType,
    val: Value,
}

impl NumberLiteralNode {
    pub fn new(val_type: ValueType, val: Value) -> Self {
        Self { val_type, val }
    }
}

impl ExecutorNode for NumberLiteralNode {
    fn value_type(&self) -> ValueType {
        self.val_type
    }

    fn next_scalar(&mut self, _: &mut Connection) -> Option<Value> {
        Some(self.val)
    }
}

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
    ) -> Self {
        let stream_ids: Vec<Uuid> = conn
            .indexer
            .borrow()
            .get_stream_ids(&name, &matchers)
            .into_iter()
            .collect();

        if stream_ids.is_empty() {
            panic!("No streams match selector!");
        }

        let stream_id = stream_ids[0];
        let file_paths = conn
            .indexer
            .borrow()
            .get_required_files(stream_id, start, end);

        Self {
            stream_ids,
            stream_idx: 0,
            cursor: Cursor::new(file_paths, start, end, conn.page_cache.clone(), hint).unwrap(),
            indexer: conn.indexer.clone(),
            page_cache: conn.page_cache.clone(),
            start,
            end,
            hint,
        }
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
            let file_paths = self
                .indexer
                .borrow()
                .get_required_files(stream_id, self.start, self.end);

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

pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

impl BinaryOp {
    pub fn apply(
        &self,
        lhs: Value,
        lhs_value_type: ValueType,
        rhs: Value,
        rhs_value_type: ValueType,
    ) -> Value {
        match self {
            BinaryOp::Add => lhs.add(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Subtract => lhs.sub(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Multiply => lhs.mul(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Divide => lhs.try_div(lhs_value_type, &rhs, rhs_value_type).unwrap(),
            BinaryOp::Modulo => lhs.try_mod(lhs_value_type, &rhs, rhs_value_type).unwrap(),
        }
    }
}

pub struct BinaryOpNode {
    child: Box<TNode>,
    return_type_: ReturnType,
}

impl BinaryOpNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        match (lhs.return_type(), rhs.return_type()) {
            (ReturnType::Scalar, ReturnType::Scalar) => Self {
                child: Box::new(TNode::ScalarToScalar(ScalarToScalarNode::new(op, lhs, rhs))),
                return_type_: ReturnType::Scalar,
            },
            (ReturnType::Vector, ReturnType::Scalar) => Self {
                child: Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, lhs, rhs))),
                return_type_: ReturnType::Vector,
            },
            (ReturnType::Scalar, ReturnType::Vector) => Self {
                child: Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, rhs, lhs))),
                return_type_: ReturnType::Vector,
            },
            (ReturnType::Vector, ReturnType::Vector) => Self {
                child: Box::new(TNode::VectorToVector(VectorToVectorNode::new(op, lhs, rhs))),
                return_type_: ReturnType::Vector,
            },
        }
    }
}

impl ExecutorNode for BinaryOpNode {
    fn return_type(&self) -> ReturnType {
        self.return_type_
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        self.child.next_vector(conn)
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        self.child.next_scalar(conn)
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
    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let lhs_opt = self.lhs.next_scalar(conn);
        let rhs_opt = self.rhs.next_scalar(conn);

        match (lhs_opt, rhs_opt) {
            (Some(lhs_value), Some(rhs_value)) => Some(self.op.apply(
                lhs_value,
                self.lhs.value_type(),
                rhs_value,
                self.rhs.value_type(),
            )),
            _ => None,
        }
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
    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        let vector_opt = self.vector_node.next_vector(conn);

        let scalar = match self.scalar {
            Some(s) => s,
            None => {
                self.scalar = self.scalar_node.next_scalar(conn);
                self.scalar.unwrap()
            }
        };

        if let Some(Vector { timestamp, value }) = vector_opt {
            Some(Vector {
                timestamp,
                value: self.op.apply(
                    value,
                    self.vector_node.value_type(),
                    scalar,
                    self.scalar_node.value_type(),
                ),
            })
        } else {
            None
        }
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
    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        let lhs_vector = self.lhs.next_vector(conn);
        let rhs_vector = self.rhs.next_vector(conn);

        if lhs_vector.is_none() || rhs_vector.is_none() {
            return None;
        }

        let Vector {
            timestamp: lhs_timestamp,
            value: lhs_value,
        } = lhs_vector.unwrap();
        let Vector {
            timestamp: rhs_timestamp,
            value: rhs_value,
        } = rhs_vector.unwrap();

        if lhs_timestamp != rhs_timestamp {
            todo!("Timestamps don't match!");
        }

        Some(Vector {
            timestamp: lhs_timestamp,
            value: self.op.apply(
                lhs_value,
                self.lhs.value_type(),
                rhs_value,
                self.rhs.value_type(),
            ),
        })
    }
}

#[derive(PartialEq)]
pub enum AggregateType {
    Sum,
    Count,
    Min,
    Max,
}

pub struct AggregateNode {
    aggregate_type: AggregateType,
    child: Box<TNode>,
}

impl AggregateNode {
    pub fn new(aggregate_type: AggregateType, child: Box<TNode>) -> Self {
        Self {
            aggregate_type,
            child,
        }
    }
}

impl ExecutorNode for AggregateNode {
    fn value_type(&self) -> ValueType {
        match self.aggregate_type {
            AggregateType::Count => match *self.child {
                TNode::VectorSelect(_) => self.child.value_type(),
                _ => ValueType::UInteger64,
            },
            _ => self.child.value_type(),
        }
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self.aggregate_type {
            AggregateType::Sum => {
                let first_vector = self.child.next_vector(conn);

                first_vector?;

                let mut sum = first_vector.unwrap().value;

                while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                    sum = sum.add_same(self.value_type(), &value);
                }

                Some(sum)
            }
            AggregateType::Count => {
                let first_vector = self.child.next_vector(conn);

                first_vector?;

                if let TNode::VectorSelect(_) = *self.child {
                    let mut count = first_vector.unwrap().value;
                    while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                        count = count.add_same(self.value_type(), &value);
                    }
                    Some(count)
                } else {
                    let mut count = 1u64;
                    while self.child.next_vector(conn).is_some() {
                        count += 1;
                    }
                    Some(count.into())
                }
            }
            AggregateType::Min | AggregateType::Max => {
                let mut is_first_value = true;
                let mut val = Value::get_default(self.value_type());

                while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                    if is_first_value {
                        val = value;
                        is_first_value = false;
                    }

                    if self.aggregate_type == AggregateType::Min {
                        val = val.min_same(self.value_type(), &value);
                    } else if self.aggregate_type == AggregateType::Max {
                        val = val.max_same(self.value_type(), &value);
                    }
                }

                if is_first_value {
                    None
                } else {
                    Some(val)
                }
            }
        }
    }
}

pub struct AverageNode {
    sum: Box<AggregateNode>,
    count: Box<AggregateNode>,
}

impl AverageNode {
    pub fn try_new(sum: Box<AggregateNode>, count: Box<AggregateNode>) -> Result<Self, ()> {
        if sum.aggregate_type != AggregateType::Sum {
            return Err(());
        }
        if count.aggregate_type != AggregateType::Count {
            return Err(());
        }

        Ok(Self { sum, count })
    }
}

impl ExecutorNode for AverageNode {
    fn value_type(&self) -> ValueType {
        ValueType::Float64
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        let sum_opt = self.sum.next_scalar(conn);
        let count_opt = self.count.next_scalar(conn);

        match (sum_opt, count_opt) {
            (Some(sum), Some(count)) => {
                sum.try_div(self.sum.value_type(), &count, self.count.value_type())
            }
            _ => None,
        }
    }
}

struct TypeValuePair(ValueType, Value);

impl PartialEq for TypeValuePair {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(self.0, &other.1, other.0)
    }
}

impl Eq for TypeValuePair {}

impl PartialOrd for TypeValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TypeValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.partial_cmp(self.0, &other.1, other.0).unwrap()
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum GetKType {
    Bottom,
    Top,
}

pub struct GetKNode {
    ix: usize,
    ks: Vec<Value>,
}

impl GetKNode {
    pub fn new(
        conn: &mut Connection,
        getk_type: GetKType,
        mut child: Box<TNode>,
        mut param: Box<TNode>,
    ) -> Self {
        let k = param.next_scalar(conn).unwrap();
        let k: usize = (k.try_convert_into_i64(param.value_type()).unwrap())
            .try_into()
            .unwrap();

        Self {
            ix: 0,
            ks: if getk_type == GetKType::Bottom {
                let mut maxheap = BinaryHeap::<TypeValuePair>::new();
                // Find (up to) k smallest values
                // Newer values overwrite older values in case of ties
                if k > 0 {
                    while let Some(Vector { value, .. }) = child.next_vector(conn) {
                        if maxheap.len() < k {
                            maxheap.push(TypeValuePair(child.value_type(), value));
                        } else {
                            let ordering =
                                value.partial_cmp_same(child.value_type(), &value).unwrap();
                            if ordering.is_le() {
                                maxheap.pop();
                                maxheap.push(TypeValuePair(child.value_type(), value));
                            }
                        }
                    }
                }
                maxheap.into_iter().map(|pair| pair.1).collect()
            } else if getk_type == GetKType::Top {
                let mut minheap = BinaryHeap::<Reverse<TypeValuePair>>::new();
                // Find (up to) k largest values
                // Newer values overwrite older values in case of ties
                if k > 0 {
                    while let Some(Vector { value, .. }) = child.next_vector(conn) {
                        if minheap.len() < k {
                            minheap.push(Reverse(TypeValuePair(child.value_type(), value)));
                        } else {
                            let ordering =
                                value.partial_cmp_same(child.value_type(), &value).unwrap();
                            if ordering.is_ge() {
                                minheap.pop();
                                minheap.push(Reverse(TypeValuePair(child.value_type(), value)));
                            }
                        }
                    }
                }
                minheap.into_iter().map(|rev_pair| rev_pair.0 .1).collect()
            } else {
                panic!("Invalid GetKType!");
            },
        }
    }
}

impl ExecutorNode for GetKNode {
    fn next_scalar(&mut self, _: &mut Connection) -> Option<Value> {
        if self.ix >= self.ks.len() {
            None
        } else {
            let next = self.ks[self.ix];
            self.ix += 1;
            Some(next)
        }
    }
}

#[cfg(test)]
mod tests {
    use promql_parser::parser;

    #[test]
    fn example_query() {
        let stmt = r#"sum(http_requests_total)"#;
        let ast = parser::parse(stmt);
        println!("{:#?}", ast);
    }
}
