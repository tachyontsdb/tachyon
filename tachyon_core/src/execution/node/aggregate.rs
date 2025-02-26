use std::time::Duration;

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
    peeked_vector: Option<Vector>, // Next vector is stored here if looked at but not returned
    end_timestamp: Timestamp,      // End timestamp of aggregation (sub)period
    done: bool,
}

pub struct AggregateNode {
    pub aggregate_type: AggregateType,
    subperiod: Option<Duration>,
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
            child: AggregateChild {
                node: child,
                peeked_vector: None,
                end_timestamp: curr_end,
                done: false,
            },
            other_child: other_child.map(|other_child| AggregateChild {
                node: other_child,
                peeked_vector: None,
                end_timestamp: curr_end,
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

    fn child_next_vector(
        child: &mut AggregateChild,
        subperiod: Option<Duration>,
        conn: &mut Connection,
    ) -> Option<Vector> {
        let next_vector = if let Some(vector) = child.peeked_vector {
            child.peeked_vector = None;
            Some(vector)
        } else {
            child.node.next_vector(conn)
        };

        if let Some(vector) = next_vector {
            if vector.timestamp > child.end_timestamp {
                child.peeked_vector = Some(vector);
                if let Some(subperiod) = subperiod {
                    child.end_timestamp += subperiod.as_millis() as u64;
                }
                None
            } else {
                Some(vector)
            }
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
        if child.done {
            return None;
        }
        let value_type = child.node.value_type();

        let mut sum = if subperiod.is_some() {
            Value::get_default(value_type)
        } else {
            child.node.next_vector(conn)?.value
        };

        while let Some(Vector { value, .. }) =
            AggregateNode::child_next_vector(child, subperiod, conn)
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
                AggregateNode::child_next_vector(child, subperiod, conn)
            {
                count = count.add_same(value_type, &value);
            }
            Some(count)
        } else {
            let mut count = 0u64;
            while AggregateNode::child_next_vector(child, subperiod, conn).is_some() {
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
        ReturnType::Scalar
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

                // SAFETY: we always create other_child when AggregateType is Average
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
                    (Some(sum), Some(count)) => {
                        Some(sum.div(sum_value_type, &count, count_value_type))
                    }
                    _ => None,
                }
            }
            AggregateType::Min | AggregateType::Max => {
                let value_type = self.value_type();
                let mut val =
                    AggregateNode::child_next_vector(&mut self.child, self.subperiod, conn)?.value;

                while let Some(Vector { value, .. }) =
                    AggregateNode::child_next_vector(&mut self.child, self.subperiod, conn)
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
}
