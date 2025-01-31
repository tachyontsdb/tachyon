use crate::{Connection, ReturnType, Value, ValueType, Vector};

use super::{ExecutorNode, TNode};

#[derive(PartialEq)]
pub enum AggregateType {
    Sum,
    Count,
    Min,
    Max,
    Average,
}

pub struct AggregateNode {
    pub aggregate_type: AggregateType,
    child: Box<TNode>,
    other_child: Option<Box<TNode>>,
}

impl AggregateNode {
    pub fn new(
        aggregate_type: AggregateType,
        child: Box<TNode>,
        other_child: Option<Box<TNode>>,
    ) -> Self {
        Self {
            aggregate_type,
            child,
            other_child,
        }
    }

    fn next_sum(
        conn: &mut Connection,
        value_type: ValueType,
        child: &mut Box<TNode>,
    ) -> Option<Value> {
        let mut sum = child.next_vector(conn)?.value;

        while let Some(Vector { value, .. }) = child.next_vector(conn) {
            sum = sum.add_same(value_type, &value);
        }

        Some(sum)
    }

    fn next_count(
        conn: &mut Connection,
        value_type: ValueType,
        child: &mut Box<TNode>,
    ) -> Option<Value> {
        let first_vector = child.next_vector(conn)?;

        if let TNode::VectorSelect(_) = **child {
            let mut count = first_vector.value;
            while let Some(Vector { value, .. }) = child.next_vector(conn) {
                count = count.add_same(value_type, &value);
            }
            Some(count)
        } else {
            let mut count = 1u64;
            while child.next_vector(conn).is_some() {
                count += 1;
            }
            Some(count.into())
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
            AggregateType::Average => ValueType::Float64,
            _ => child_value_type,
        }
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self.aggregate_type {
            AggregateType::Sum => AggregateNode::next_sum(conn, self.value_type(), &mut self.child),
            AggregateType::Count => {
                AggregateNode::next_count(conn, self.value_type(), &mut self.child)
            }
            AggregateType::Average => {
                let sum_opt = AggregateNode::next_sum(conn, self.value_type(), &mut self.child);
                let count_opt = AggregateNode::next_count(
                    conn,
                    self.value_type(),
                    self.other_child
                        .as_mut()
                        .expect("invalid initialization of child nodes of AggregateType::Average"), // Should never happen
                );

                match (sum_opt, count_opt) {
                    (Some(sum), Some(count)) => {
                        Some(sum.div(self.value_type(), &count, self.value_type()))
                    }
                    _ => None,
                }
            }
            AggregateType::Min | AggregateType::Max => {
                let value_type = self.value_type();
                let mut val = self.child.next_vector(conn)?.value;

                while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                    if self.aggregate_type == AggregateType::Min {
                        val = val.min_same(value_type, &value);
                    } else if self.aggregate_type == AggregateType::Max {
                        val = val.max_same(value_type, &value);
                    }
                }

                Some(val)
            }
        }
    }
}
