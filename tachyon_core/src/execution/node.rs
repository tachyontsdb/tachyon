use crate::query::indexer::Indexer;
use crate::storage::file::{Cursor, ScanHint};
use crate::storage::page_cache::PageCache;
use crate::{Connection, ReturnType, Timestamp, Value, ValueType, Vector};
use promql_parser::label::Matchers;
use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::rc::Rc;
use uuid::Uuid;

pub trait ExecutorNode {
    fn value_type(&self) -> ValueType;
    fn return_type(&self) -> ReturnType;

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
    extracted_literal: bool,
}

impl NumberLiteralNode {
    pub fn new(val_type: ValueType, val: Value) -> Self {
        Self {
            val_type,
            val,
            extracted_literal: false,
        }
    }
}

impl ExecutorNode for NumberLiteralNode {
    fn value_type(&self) -> ValueType {
        self.val_type
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, _: &mut Connection) -> Option<Value> {
        if self.extracted_literal {
            None
        } else {
            self.extracted_literal = true;
            Some(self.val)
        }
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

#[derive(Debug)]
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
            BinaryOp::Divide => lhs.div(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Modulo => lhs.mdl(lhs_value_type, &rhs, rhs_value_type),
        }
    }
}

pub struct BinaryOpNode {
    child: Box<TNode>,
}

impl BinaryOpNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        match (lhs.return_type(), rhs.return_type()) {
            (ReturnType::Scalar, ReturnType::Scalar) => Self {
                child: Box::new(TNode::ScalarToScalar(ScalarToScalarNode::new(op, lhs, rhs))),
            },
            (ReturnType::Vector, ReturnType::Scalar) => Self {
                child: Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, lhs, rhs))),
            },
            (ReturnType::Scalar, ReturnType::Vector) => Self {
                child: Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, rhs, lhs))),
            },
            (ReturnType::Vector, ReturnType::Vector) => Self {
                child: Box::new(TNode::VectorToVector(VectorToVectorNode::new(op, lhs, rhs))),
            },
        }
    }
}

impl ExecutorNode for BinaryOpNode {
    fn value_type(&self) -> ValueType {
        self.child.value_type()
    }

    fn return_type(&self) -> ReturnType {
        self.child.return_type()
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
    fn value_type(&self) -> ValueType {
        let lhs_value_type = self.lhs.value_type();

        if lhs_value_type != self.rhs.value_type() {
            todo!("Implement operations between different types!");
        }

        lhs_value_type
    }

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
    fn value_type(&self) -> ValueType {
        let vector_value_type = self.vector_node.value_type();

        if vector_value_type != self.scalar_node.value_type() {
            todo!("Implement operations between different types!");
        }

        vector_value_type
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Vector
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
    lhs_range: VecDeque<Vector>,
    rhs_range: VecDeque<Vector>,
    value_opt: Option<(Vector, VectorToVectorStream)>,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum VectorToVectorStream {
    Lhs = 0,
    Rhs = 1,
}

impl VectorToVectorNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        if lhs.value_type() == ValueType::Float64 || rhs.value_type() == ValueType::Float64 {
            todo!("Floats not supported yet");
        }

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
            range[0].value
        } else if range.len() == 2 {
            let (v1, v2) = (range[1].value, range[0].value);
            let (t1, t2) = (range[1].timestamp, range[0].timestamp);

            let rhs_value_type = self.rhs.value_type();

            let slope = (v2.convert_into_f64(self.lhs.value_type())
                - v1.convert_into_f64(rhs_value_type))
                / (t2 as f64 - t1 as f64);
            let res =
                ((ts as f64 - t1 as f64) * slope + v1.convert_into_f64(rhs_value_type)).round();

            // TODO: Allow floats
            (res as u64).into()
        } else {
            panic!("No values in range for interpolation.")
        }
    }

    fn next_child_vector(
        &mut self,
        conn: &mut Connection,
        stream: VectorToVectorStream,
    ) -> Option<Vector> {
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
    fn value_type(&self) -> ValueType {
        let lhs_value_type = self.lhs.value_type();

        if lhs_value_type != self.rhs.value_type() {
            todo!("Implement operations between different types!");
        }

        lhs_value_type
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Vector
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        // Initial case
        if self.lhs_range.is_empty() && self.rhs_range.is_empty() && self.value_opt.is_none() {
            let lhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs);
            let rhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs);

            match (lhs_vector_opt, rhs_vector_opt) {
                (Some(lhs), Some(rhs)) => {
                    let Vector {
                        timestamp: lhs_ts,
                        value: lhs_val,
                    } = lhs;
                    let Vector {
                        timestamp: rhs_ts,
                        value: rhs_val,
                    } = rhs;
                    match lhs_ts.cmp(&rhs_ts) {
                        std::cmp::Ordering::Less => {
                            // Store this value
                            self.value_opt = Some((rhs, VectorToVectorStream::Rhs));
                        }
                        std::cmp::Ordering::Greater => {
                            // Store this value
                            self.value_opt = Some((lhs, VectorToVectorStream::Lhs));
                        }
                        std::cmp::Ordering::Equal => {}
                    }

                    Some(Vector {
                        timestamp: Timestamp::min(lhs_ts, rhs_ts),
                        value: self.op.apply(
                            lhs_val,
                            self.lhs.value_type(),
                            rhs_val,
                            self.rhs.value_type(),
                        ), // These are the first values of the stream thus no interpolation is necessary
                    })
                }
                _ => None, // One of the streams is empty. Then do nothing.
            }
        } else if self.value_opt.is_some() {
            // There is a residual value from one stream
            let (val, last_stream) = self.value_opt.unwrap();
            let Vector {
                timestamp: value_ts,
                value: value_val,
            } = val;
            match last_stream {
                VectorToVectorStream::Lhs => {
                    let rhs_vec_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs); // Fetch value from opposing stream
                    match rhs_vec_opt {
                        Some(Vector {
                            timestamp: rhs_ts,
                            value: rhs_val,
                        }) => {
                            match rhs_ts.cmp(&value_ts) {
                                // If the new vector is less than the residual we must interpolate the residual value relative to the new vector
                                std::cmp::Ordering::Less => {
                                    let lhs_interpolated = self
                                        .calculate_value_with_linear_interpolation(
                                            rhs_ts,
                                            VectorToVectorStream::Lhs,
                                        );
                                    Some(Vector {
                                        timestamp: rhs_ts,
                                        value: self.op.apply(
                                            lhs_interpolated,
                                            ValueType::UInteger64, // TODO: Fix this
                                            rhs_val,
                                            self.rhs.value_type(),
                                        ),
                                    })
                                }
                                // If the new vector is greater than the residual we must store it, and then interpolate it relative to the residual
                                std::cmp::Ordering::Greater => {
                                    self.value_opt =
                                        Some((rhs_vec_opt.unwrap(), VectorToVectorStream::Rhs));
                                    let rhs_interpolated = self
                                        .calculate_value_with_linear_interpolation(
                                            value_ts,
                                            VectorToVectorStream::Rhs,
                                        );
                                    Some(Vector {
                                        timestamp: value_ts,
                                        value: self.op.apply(
                                            value_val,
                                            self.value_type(),
                                            rhs_interpolated,
                                            ValueType::UInteger64,
                                        ),
                                    })
                                }
                                // If they are equal then no interpolation needed, we can discard residual value
                                std::cmp::Ordering::Equal => {
                                    self.value_opt = None;
                                    Some(Vector {
                                        timestamp: value_ts,
                                        value: self.op.apply(
                                            value_val,
                                            self.lhs.value_type(),
                                            rhs_val,
                                            self.rhs.value_type(),
                                        ),
                                    })
                                }
                            }
                        }
                        _ => {
                            let rhs_interpolated = self.rhs_range[0].value; // If there is no value from the RHS stream, then we must interpolate based on the last value
                            self.value_opt = None;
                            Some(Vector {
                                timestamp: value_ts,
                                value: self.op.apply(
                                    value_val,
                                    self.lhs.value_type(),
                                    rhs_interpolated,
                                    self.rhs.value_type(),
                                ),
                            })
                        }
                    }
                }
                VectorToVectorStream::Rhs => {
                    let lhs_vec_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs); // Fetch value from opposing stream
                    match lhs_vec_opt {
                        Some(Vector {
                            timestamp: lhs_ts,
                            value: lhs_val,
                        }) => match lhs_ts.cmp(&value_ts) {
                            // If the new vector is less than the residual we must interpolate the residual value relative to the new vector
                            std::cmp::Ordering::Less => {
                                let rhs_interpolated = self
                                    .calculate_value_with_linear_interpolation(
                                        lhs_ts,
                                        VectorToVectorStream::Rhs,
                                    );
                                Some(Vector {
                                    timestamp: lhs_ts,
                                    value: self.op.apply(
                                        lhs_val,
                                        self.lhs.value_type(),
                                        rhs_interpolated,
                                        ValueType::UInteger64,
                                    ), // TODO: Fix this
                                })
                            }
                            // If the new vector is greater than the residual we must store it, and then interpolate it relative to the residual
                            std::cmp::Ordering::Greater => {
                                self.value_opt =
                                    Some((lhs_vec_opt.unwrap(), VectorToVectorStream::Lhs));
                                let lhs_interpolated = self
                                    .calculate_value_with_linear_interpolation(
                                        value_ts,
                                        VectorToVectorStream::Lhs,
                                    );
                                Some(Vector {
                                    timestamp: value_ts,
                                    value: self.op.apply(
                                        lhs_interpolated,
                                        ValueType::UInteger64,
                                        value_val,
                                        self.rhs.value_type(),
                                    ),
                                })
                            }
                            // If they are equal then no interpolation needed, we can discard residual value
                            std::cmp::Ordering::Equal => {
                                self.value_opt = None;
                                Some(Vector {
                                    timestamp: value_ts,
                                    value: self.op.apply(
                                        lhs_val,
                                        self.lhs.value_type(),
                                        value_val,
                                        self.rhs.value_type(),
                                    ),
                                })
                            }
                        },
                        _ => {
                            let lhs_interpolated = self.lhs_range[0].value; // If there is no value from the LHS stream, then we must interpolate based on the last value
                            self.value_opt = None;
                            Some(Vector {
                                timestamp: value_ts,
                                value: self.op.apply(
                                    lhs_interpolated,
                                    ValueType::UInteger64, // TODO: Fix this
                                    value_val,
                                    self.rhs.value_type(),
                                ),
                            })
                        }
                    }
                }
            }
        } else {
            // There is no residual value present
            let lhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Lhs);
            let rhs_vector_opt = self.next_child_vector(conn, VectorToVectorStream::Rhs);

            match (lhs_vector_opt, rhs_vector_opt) {
                (
                    Some(Vector {
                        timestamp: lhs_ts,
                        value: lhs_val,
                    }),
                    Some(Vector {
                        timestamp: rhs_ts,
                        value: rhs_val,
                    }),
                ) => match lhs_ts.cmp(&rhs_ts) {
                    // If the LHS is less than the RHS we must store the RHS and interpolate it relative to the LHS
                    std::cmp::Ordering::Less => {
                        self.value_opt = Some((rhs_vector_opt.unwrap(), VectorToVectorStream::Rhs));
                        let rhs_interpolated = self.calculate_value_with_linear_interpolation(
                            lhs_ts,
                            VectorToVectorStream::Rhs,
                        );
                        Some(Vector {
                            timestamp: lhs_ts,
                            value: self.op.apply(
                                lhs_val,
                                self.lhs.value_type(),
                                rhs_interpolated,
                                ValueType::UInteger64, // TODO: Fix this
                            ),
                        })
                    }
                    // If the LHS is greater than the RHS we must store the LHS and interpolate it relative to the RHS
                    std::cmp::Ordering::Greater => {
                        self.value_opt = Some((lhs_vector_opt.unwrap(), VectorToVectorStream::Lhs));
                        let lhs_interpolated = self.calculate_value_with_linear_interpolation(
                            rhs_ts,
                            VectorToVectorStream::Lhs,
                        );
                        Some(Vector {
                            timestamp: rhs_ts,
                            value: self.op.apply(
                                lhs_interpolated,
                                ValueType::UInteger64,
                                rhs_val,
                                self.rhs.value_type(),
                            ),
                        })
                    }
                    // If equal then no interpolation necessary
                    std::cmp::Ordering::Equal => Some(Vector {
                        timestamp: lhs_ts,
                        value: self.op.apply(
                            lhs_val,
                            self.lhs.value_type(),
                            rhs_val,
                            self.value_type(),
                        ),
                    }),
                },
                // If no RHS value then we interpolate it based on the last one
                (
                    Some(Vector {
                        timestamp: lhs_ts,
                        value: lhs_val,
                    }),
                    None,
                ) => {
                    let rhs_interpolated = self.rhs_range[0].value;
                    Some(Vector {
                        timestamp: lhs_ts,
                        value: self.op.apply(
                            lhs_val,
                            self.value_type(),
                            rhs_interpolated,
                            ValueType::UInteger64,
                        ), // TODO: Fix this for floats
                    })
                }
                // If no LHS value then we interpolate it based on the last one
                (
                    None,
                    Some(Vector {
                        timestamp: rhs_ts,
                        value: rhs_val,
                    }),
                ) => {
                    let lhs_interpolated = self.lhs_range[0].value;
                    Some(Vector {
                        timestamp: rhs_ts,
                        value: self.op.apply(
                            lhs_interpolated,
                            ValueType::UInteger64,
                            rhs_val,
                            self.rhs.value_type(),
                        ),
                    })
                }
                _ => None, // Both of the streams are empty.
            }
        }
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
        let child_value_type = self.child.value_type();

        match self.aggregate_type {
            AggregateType::Count => match *self.child {
                TNode::VectorSelect(_) => child_value_type,
                _ => ValueType::UInteger64,
            },
            _ => child_value_type,
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

                let value_type = self.value_type();

                while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                    sum = sum.add_same(value_type, &value);
                }

                Some(sum)
            }
            AggregateType::Count => {
                let first_vector = self.child.next_vector(conn);

                first_vector?;

                if let TNode::VectorSelect(_) = *self.child {
                    let mut count = first_vector.unwrap().value;
                    let value_type = self.value_type();
                    while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                        count = count.add_same(value_type, &value);
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
                let value_type = self.value_type();
                let mut val = Value::get_default(value_type);

                while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                    if is_first_value {
                        val = value;
                        is_first_value = false;
                    }

                    if self.aggregate_type == AggregateType::Min {
                        val = val.min_same(value_type, &value);
                    } else if self.aggregate_type == AggregateType::Max {
                        val = val.max_same(value_type, &value);
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
                Some(sum.div(self.sum.value_type(), &count, self.count.value_type()))
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
    getk_type: GetKType,
    child: Box<TNode>,
    param: Box<TNode>,

    k: Option<usize>,

    ix: usize,
    ks: Option<Vec<Value>>,
}

impl GetKNode {
    pub fn new(
        _: &mut Connection,
        getk_type: GetKType,
        child: Box<TNode>,
        param: Box<TNode>,
    ) -> Self {
        Self {
            getk_type,
            child,
            param,
            k: None,
            ks: None,
            ix: 0,
        }
    }
}

impl ExecutorNode for GetKNode {
    fn value_type(&self) -> ValueType {
        self.child.value_type()
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        if self.k.is_none() {
            // Generate heaps during the first call

            let k = ((self.param.next_scalar(conn).unwrap())
                .convert_into_u64(self.param.value_type())) as usize;
            self.k = Some(k);

            self.ks = Some(if k == 0 {
                Vec::new()
            } else {
                let child_value_type = self.child.value_type();

                // Newer values overwrite older values in case of ties

                if self.getk_type == GetKType::Bottom {
                    let mut maxheap = BinaryHeap::<TypeValuePair>::new();
                    while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                        if maxheap.len() < k {
                            maxheap.push(TypeValuePair(child_value_type, value));
                        } else {
                            let ordering = value
                                .partial_cmp_same(child_value_type, &maxheap.peek().unwrap().1)
                                .unwrap();
                            if ordering.is_le() {
                                maxheap.pop();
                                maxheap.push(TypeValuePair(child_value_type, value));
                            }
                        }
                    }
                    maxheap
                        .into_sorted_vec()
                        .into_iter()
                        .map(|pair| pair.1)
                        .collect()
                } else {
                    let mut minheap = BinaryHeap::<Reverse<TypeValuePair>>::new();
                    while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                        if minheap.len() < k {
                            minheap.push(Reverse(TypeValuePair(child_value_type, value)));
                        } else {
                            let ordering = value
                                .partial_cmp_same(child_value_type, &minheap.peek().unwrap().0 .1)
                                .unwrap();
                            if ordering.is_ge() {
                                minheap.pop();
                                minheap.push(Reverse(TypeValuePair(child_value_type, value)));
                            }
                        }
                    }
                    minheap
                        .into_sorted_vec()
                        .into_iter()
                        .map(|rev_pair| rev_pair.0 .1)
                        .collect()
                }
            });
        }

        let ks = self.ks.as_ref().unwrap();

        if self.ix >= ks.len() {
            None
        } else {
            let value = ks[self.ix];
            self.ix += 1;
            Some(value)
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
