use promql_parser::label::Matcher;
use promql_parser::parser::{
    self, AggregateExpr, BinaryExpr, Call, Expr, Extension, MatrixSelector, NumberLiteral,
    ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr, VectorSelector,
};

use crate::api::Connection;
use crate::common::{Timestamp, Value};
use crate::executor::node::{
    AverageNode, CountNode, MaxNode, MinNode, SumNode, TNode, VectorBinaryOp, VectorBinaryOpNode,
    VectorSelectNode,
};
use crate::executor::{self, execute, Context, OperationCode::*};
use crate::storage::file::ScanHint;

#[derive(Debug)]
pub struct QueryPlanner<'a> {
    cursor_idx: u64,
    ast: &'a Expr,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ast: &'a Expr, start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        Self {
            cursor_idx: 0,
            ast,
            start,
            end,
        }
    }

    pub fn plan(&mut self, conn: &mut Connection) -> TNode {
        self.handle_expr(self.ast, conn, ScanHint::None).unwrap()
    }

    fn handle_aggregate_expr(
        &mut self,
        expr: &AggregateExpr,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        match expr.op.id() {
            parser::token::T_SUM => Ok(TNode::Sum(SumNode::new(Box::new(
                self.handle_expr(&expr.expr, conn, ScanHint::Sum).unwrap(),
            )))),
            parser::token::T_COUNT => Ok(TNode::Count(CountNode::new(Box::new(
                self.handle_expr(&expr.expr, conn, ScanHint::Count).unwrap(),
            )))),
            parser::token::T_AVG => Ok(TNode::Average(AverageNode::new(
                Box::new(SumNode::new(Box::new(
                    self.handle_expr(&expr.expr, conn, ScanHint::Sum).unwrap(),
                ))),
                Box::new(CountNode::new(Box::new(
                    self.handle_expr(&expr.expr, conn, ScanHint::Count).unwrap(),
                ))),
            ))),
            parser::token::T_MIN => Ok(TNode::Min(MinNode::new(Box::new(
                self.handle_expr(&expr.expr, conn, ScanHint::Min).unwrap(),
            )))),
            parser::token::T_MAX => Ok(TNode::Max(MaxNode::new(Box::new(
                self.handle_expr(&expr.expr, conn, ScanHint::Max).unwrap(),
            )))),
            _ => panic!("Unknown aggregation token."),
        }
    }

    fn handle_unary_expr(
        &mut self,
        expr: &UnaryExpr,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        let result = self.handle_expr(&expr.expr, conn, ScanHint::None);
        todo!()
    }

    fn handle_binary_expr(
        &mut self,
        expr: &BinaryExpr,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Ok(TNode::VectorBinaryOp(VectorBinaryOpNode::new(
            match expr.op.id() {
                parser::token::T_ADD => VectorBinaryOp::Add,
                parser::token::T_SUB => VectorBinaryOp::Subtract,
                parser::token::T_MUL => VectorBinaryOp::Multiply,
                parser::token::T_DIV => VectorBinaryOp::Divide,
                parser::token::T_MOD => VectorBinaryOp::Modulo,
                _ => panic!("Unknown aggregation token."),
            },
            Box::new(self.handle_expr(&expr.lhs, conn, ScanHint::None).unwrap()),
            Box::new(self.handle_expr(&expr.rhs, conn, ScanHint::None).unwrap()),
        )))
    }

    fn handle_paren_expr(
        &mut self,
        expr: &ParenExpr,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        self.handle_expr(&expr.expr, conn, ScanHint::None)
    }

    fn handle_subquery_expr(
        &mut self,
        expr: &SubqueryExpr,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("Subquery expressions currently not supported.")
    }

    fn handle_number_literal_expr(
        &mut self,
        expr: &NumberLiteral,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        todo!()
    }

    fn handle_string_literal_expr(
        &mut self,
        expr: &StringLiteral,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        todo!()
    }

    fn handle_vector_selector_expr(
        &mut self,
        expr: &VectorSelector,
        conn: &mut Connection,
        hint: ScanHint,
    ) -> Result<TNode, &'static str> {
        let start = if expr.at.is_some() {
            let mut at_res = match expr.at.as_ref().unwrap() {
                parser::AtModifier::Start => 0,
                parser::AtModifier::End => u64::MAX,
                parser::AtModifier::At(t) => t.elapsed().unwrap().as_millis() as u64,
            };

            if expr.offset.is_some() {
                at_res = match expr.offset.as_ref().unwrap() {
                    parser::Offset::Pos(t) => at_res.saturating_add(t.as_millis() as u64),
                    parser::Offset::Neg(t) => at_res.saturating_sub(t.as_millis() as u64),
                }
            }
            at_res
        } else {
            self.start.unwrap()
        };

        let end = self.end.unwrap();

        Ok(TNode::VectorSelect(VectorSelectNode::new(
            conn,
            expr.name.clone().unwrap(),
            expr.matchers.clone(),
            start,
            end,
            hint,
        )))
    }

    fn handle_matrix_selector_expr(
        &mut self,
        expr: &MatrixSelector,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        todo!()
    }

    fn handle_call_expr(
        &mut self,
        expr: &Call,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("Call expressions currently not supported.")
    }

    fn handle_extension_expr(
        &mut self,
        expr: &Extension,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("Extension expressions currently not supported.")
    }

    fn handle_expr(
        &mut self,
        expr: &Expr,
        conn: &mut Connection,
        hint: ScanHint,
    ) -> Result<TNode, &'static str> {
        match expr {
            Expr::Aggregate(aggregate_expr) => self.handle_aggregate_expr(aggregate_expr, conn),
            Expr::Unary(unary_expr) => self.handle_unary_expr(unary_expr, conn),
            Expr::Binary(binary_expr) => self.handle_binary_expr(binary_expr, conn),
            Expr::Paren(paren_expr) => self.handle_paren_expr(paren_expr, conn),
            Expr::Subquery(subquery_expr) => self.handle_subquery_expr(subquery_expr, conn),
            Expr::NumberLiteral(number_literal_expr) => {
                self.handle_number_literal_expr(number_literal_expr, conn)
            }
            Expr::StringLiteral(string_literal_expr) => {
                self.handle_string_literal_expr(string_literal_expr, conn)
            }
            Expr::VectorSelector(vector_selector_expr) => {
                self.handle_vector_selector_expr(vector_selector_expr, conn, hint)
            }
            Expr::MatrixSelector(matrix_selector_expr) => {
                self.handle_matrix_selector_expr(matrix_selector_expr, conn)
            }
            Expr::Call(call_expr) => self.handle_call_expr(call_expr, conn),
            Expr::Extension(extension_expr) => self.handle_extension_expr(extension_expr, conn),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_selector_query() {
        let query_string = r#"http_requests_total{service = "web" or service = "nice"} @ 324"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::VectorSelector(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not a vector selector");
            }
        };
    }

    #[test]
    fn test_sum_query() {
        let query_string = r#"sum(http_requests_total{service = "web" or service = "nice"})"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::Aggregate(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not an aggregate");
            }
        };
    }

    #[test]
    fn test_count_query() {
        let query_string = r#"count(http_requests_total{service = "web" or service = "nice"})"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::Aggregate(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not an aggregate");
            }
        };
    }

    #[test]
    fn test_avg_query() {
        let query_string = r#"avg(http_requests_total{service = "web" or service = "nice"})"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::Aggregate(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not an aggregate");
            }
        };
    }

    #[test]
    fn test_min_query() {
        let query_string = r#"min(http_requests_total{service = "web" or service = "nice"})"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::Aggregate(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not an aggregate");
            }
        };
    }

    #[test]
    fn test_max_query() {
        let query_string = r#"max(http_requests_total{service = "web" or service = "nice"})"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::Aggregate(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not an aggregate");
            }
        };
    }
}
