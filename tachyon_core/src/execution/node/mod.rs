use crate::{Connection, ReturnType, Value, ValueType, Vector};

mod aggregate;
mod average;
mod binary_op;
mod get_k;
mod number_literal;
mod scalar_to_scalar;
mod vector_select;
mod vector_to_scalar;
mod vector_to_vector;

pub use aggregate::*;
pub use average::*;
pub use binary_op::*;
pub use get_k::*;
pub use number_literal::*;
pub use scalar_to_scalar::*;
pub use vector_select::*;
pub use vector_to_scalar::*;
pub use vector_to_vector::*;

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
