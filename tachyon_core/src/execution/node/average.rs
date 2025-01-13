use crate::{Connection, ReturnType, Value, ValueType};

use super::{AggregateNode, AggregateType, ExecutorNode};

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
