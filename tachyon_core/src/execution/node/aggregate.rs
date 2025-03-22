use std::{cmp, time::Duration};

use crate::{Connection, ReturnType, Timestamp, Value, ValueType, Vector};

use super::{ExecutorNode, TNode};

#[derive(PartialEq)]
pub enum AggregateType {
    Sum,
    Count,
    Min,
    Max,
    Average,
}

struct AggregateChild {
    node: Box<TNode>,
    /// Next vector is stored here if looked at but not returned
    peeked_vector: Option<Vector>,
    /// End timestamp of aggregation (sub)period
    end: Timestamp,
    /// Set to true when all vectors have been read
    done: bool,
}

pub struct AggregateNode {
    pub aggregate_type: AggregateType,
    subperiod: Option<Duration>,
    end: Timestamp,
    child: AggregateChild,
    other_child: Option<AggregateChild>,
}

impl AggregateNode {
    pub fn new(
        aggregate_type: AggregateType,
        subperiod: Option<Duration>,
        start: Timestamp,
        end: Timestamp,
        child: Box<TNode>,
        other_child: Option<Box<TNode>>,
    ) -> Self {
        let curr_end = if let Some(subperiod) = subperiod {
            start + (subperiod.as_millis() as u64)
        } else {
            end
        };

        Self {
            aggregate_type,
            subperiod,
            end,
            child: AggregateChild {
                node: child,
                peeked_vector: None,
                end: curr_end,
                done: false,
            },
            other_child: other_child.map(|other_child| AggregateChild {
                node: other_child,
                peeked_vector: None,
                end: curr_end,
                done: false,
            }),
        }
    }

    fn using_scanhint(child: &AggregateChild, subperiod: Option<Duration>) -> bool {
        matches!(
            (child.node.as_ref(), subperiod),
            (TNode::VectorSelect(_), None)
        )
    }

    /// Wrapper around next_vector().
    /// When the next vector is past the current subperiod, returns None; the next call will return that vector.
    /// When there are no more vectors, returns None and sets child.done = true.
    fn next_child_vector(
        child: &mut AggregateChild,
        subperiod: Option<Duration>,
        conn: &mut Connection,
    ) -> Option<Vector> {
        // Aggregation by subperiod
        if let Some(subperiod) = subperiod {
            // Retrieve peeked vector; else, get next vector
            let next_vector = if let Some(vector) = child.peeked_vector {
                child.peeked_vector = None;
                // No need to check if this is past the query end as the planner ensures all child vectors obey start <= t <= end
                child.end += subperiod.as_millis() as u64;
                Some(vector)
            } else {
                child.node.next_vector(conn)
            };

            if let Some(vector) = next_vector {
                // Return None if next vector is past subperiod; next call will return it
                if vector.timestamp > child.end {
                    child.peeked_vector = Some(vector);
                    // child.done = true is not set here as there will be more vectors in the next subperiod
                    None
                } else {
                    Some(vector)
                }
            } else {
                child.done = true;
                None
            }

            // Regular aggregation
        } else if let Some(vector) = child.node.next_vector(conn) {
            Some(vector)
        } else {
            child.done = true;
            None
        }
    }

    fn next_sum(
        child: &mut AggregateChild,
        subperiod: Option<Duration>,
        conn: &mut Connection,
    ) -> Option<Value> {
        let value_type = child.node.value_type();
        let mut sum = AggregateNode::next_child_vector(child, subperiod, conn)?.value;

        while let Some(Vector { value, .. }) =
            AggregateNode::next_child_vector(child, subperiod, conn)
        {
            sum = sum.add_same(value_type, &value);
        }
        Some(sum)
    }

    fn next_count(
        child: &mut AggregateChild,
        subperiod: Option<Duration>,
        conn: &mut Connection,
    ) -> Option<Value> {
        if child.done {
            return None;
        }
        let value_type = child.node.value_type();

        if AggregateNode::using_scanhint(child, subperiod) {
            let mut count = Value::get_default(value_type);
            while let Some(Vector { value, .. }) =
                AggregateNode::next_child_vector(child, subperiod, conn)
            {
                count = count.add_same(value_type, &value);
            }
            Some(count)
        } else {
            let mut count = 0u64;
            while AggregateNode::next_child_vector(child, subperiod, conn).is_some() {
                count += 1;
            }
            Some(count.into())
        }
    }
}

impl ExecutorNode for AggregateNode {
    fn value_type(&self) -> ValueType {
        let child_value_type = self.child.node.value_type();

        match self.aggregate_type {
            AggregateType::Count => {
                if AggregateNode::using_scanhint(&self.child, self.subperiod) {
                    child_value_type
                } else {
                    ValueType::UInteger64
                }
            }
            AggregateType::Average => ValueType::Float64,
            _ => child_value_type,
        }
    }

    fn return_type(&self) -> ReturnType {
        if self.subperiod.is_some() {
            ReturnType::Vector
        } else {
            ReturnType::Scalar
        }
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self.aggregate_type {
            AggregateType::Sum => AggregateNode::next_sum(&mut self.child, self.subperiod, conn),
            AggregateType::Count => {
                AggregateNode::next_count(&mut self.child, self.subperiod, conn)
            }
            AggregateType::Average => {
                let sum_value_type = self.child.node.value_type();
                let sum_opt = AggregateNode::next_sum(&mut self.child, self.subperiod, conn);

                // SAFETY: we create other_child when AggregateType is Average
                let count_value_type = if AggregateNode::using_scanhint(
                    self.other_child.as_ref().unwrap(),
                    self.subperiod,
                ) {
                    self.other_child.as_mut().unwrap().node.value_type()
                } else {
                    ValueType::UInteger64
                };
                let count_opt = AggregateNode::next_count(
                    self.other_child.as_mut().unwrap(),
                    self.subperiod,
                    conn,
                );

                match (sum_opt, count_opt) {
                    // sum and count will either both be Some or both be None
                    (Some(sum), Some(count)) => {
                        Some(sum.div(sum_value_type, &count, count_value_type))
                    }
                    _ => None,
                }
            }
            AggregateType::Min | AggregateType::Max => {
                let value_type = self.value_type();
                let mut val =
                    AggregateNode::next_child_vector(&mut self.child, self.subperiod, conn)?.value;

                while let Some(Vector { value, .. }) =
                    AggregateNode::next_child_vector(&mut self.child, self.subperiod, conn)
                {
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

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        // While there are still more vectors to be read
        while !self.child.done {
            // If the current subperiod has an aggregate result, return it
            if let Some(value) = self.next_scalar(conn) {
                let timestamp = cmp::min(self.child.end, self.end); // Bound by end timestamp
                return Some(Vector { timestamp, value });
            }
        }
        None
    }
}
