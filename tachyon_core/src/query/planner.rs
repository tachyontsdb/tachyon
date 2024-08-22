use super::node::{
    AggregateNode, AggregateType, AverageNode, BinaryOp, BinaryOpNode, GetKNode, GetKType,
    NumberLiteralNode, TNode, VectorSelectNode,
};
use crate::storage::file::ScanHint;
use crate::{Connection, Timestamp};
use promql_parser::parser::{
    self, AggregateExpr, BinaryExpr, Call, Expr, Extension, MatrixSelector, NumberLiteral,
    ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr, VectorSelector,
};

#[derive(Debug)]
pub struct QueryPlanner<'a> {
    ast: &'a Expr,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ast: &'a Expr, start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        Self { ast, start, end }
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
            parser::token::T_SUM
            | parser::token::T_COUNT
            | parser::token::T_MIN
            | parser::token::T_MAX => Ok(TNode::Aggregate(AggregateNode::new(
                match expr.op.id() {
                    parser::token::T_SUM => AggregateType::Sum,
                    parser::token::T_COUNT => AggregateType::Count,
                    parser::token::T_MIN => AggregateType::Min,
                    parser::token::T_MAX => AggregateType::Max,
                    _ => panic!("Unknown aggregation token!"),
                },
                Box::new(
                    self.handle_expr(
                        &expr.expr,
                        conn,
                        match expr.op.id() {
                            parser::token::T_SUM => ScanHint::Sum,
                            parser::token::T_COUNT => ScanHint::Count,
                            parser::token::T_MIN => ScanHint::Min,
                            parser::token::T_MAX => ScanHint::Max,
                            _ => panic!("Unknown aggregation token!"),
                        },
                    )
                    .unwrap(),
                ),
            ))),
            parser::token::T_AVG => Ok(TNode::Average(
                AverageNode::try_new(
                    Box::new(AggregateNode::new(
                        AggregateType::Sum,
                        Box::new(self.handle_expr(&expr.expr, conn, ScanHint::Sum).unwrap()),
                    )),
                    Box::new(AggregateNode::new(
                        AggregateType::Count,
                        Box::new(self.handle_expr(&expr.expr, conn, ScanHint::Count).unwrap()),
                    )),
                )
                .unwrap(),
            )),
            parser::token::T_BOTTOMK | parser::token::T_TOPK => {
                let child = Box::new(self.handle_expr(&expr.expr, conn, ScanHint::None).unwrap());
                let param = Box::new(
                    self.handle_expr(expr.param.as_ref().unwrap(), conn, ScanHint::None)
                        .unwrap(),
                );
                Ok(TNode::GetK(GetKNode::new(
                    conn,
                    match expr.op.id() {
                        parser::token::T_BOTTOMK => GetKType::Bottom,
                        parser::token::T_TOPK => GetKType::Top,
                        _ => panic!("Unknown aggregation token!"),
                    },
                    child,
                    param,
                )))
            }
            _ => panic!("Unknown aggregation token!"),
        }
    }

    fn handle_unary_expr(
        &mut self,
        _: &UnaryExpr,
        _: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("Unary expressions currently not supported!")
    }

    fn handle_binary_expr(
        &mut self,
        expr: &BinaryExpr,
        conn: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Ok(TNode::BinaryOp(BinaryOpNode::new(
            match expr.op.id() {
                parser::token::T_ADD => BinaryOp::Add,
                parser::token::T_SUB => BinaryOp::Subtract,
                parser::token::T_MUL => BinaryOp::Multiply,
                parser::token::T_DIV => BinaryOp::Divide,
                parser::token::T_MOD => BinaryOp::Modulo,
                _ => panic!("Unknown aggregation token!"),
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
        _: &SubqueryExpr,
        _: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("Subquery expressions currently not supported!")
    }

    fn handle_number_literal_expr(
        &mut self,
        expr: &NumberLiteral,
        _: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Ok(TNode::NumberLiteral(NumberLiteralNode::new(
            expr.val.into(),
        )))
    }

    fn handle_string_literal_expr(
        &mut self,
        _: &StringLiteral,
        _: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("String literal expressions currently not supported!")
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
        _: &MatrixSelector,
        _: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("Matrix expressions currently not supported!")
    }

    fn handle_call_expr(&mut self, _: &Call, _: &mut Connection) -> Result<TNode, &'static str> {
        Err("Call expressions currently not supported!")
    }

    fn handle_extension_expr(
        &mut self,
        _: &Extension,
        _: &mut Connection,
    ) -> Result<TNode, &'static str> {
        Err("Extension expressions currently not supported!")
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

    #[test]
    fn test_bottomk_query() {
        let query_string =
            r#"bottomk(5, http_requests_total{service = "web" or service = "nice"})"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::Aggregate(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not an aggregate");
            }
        };
    }

    #[test]
    fn test_topk_query() {
        let query_string = r#"topk(5, http_requests_total{service = "web" or service = "nice"})"#;
        let res = parser::parse(query_string).unwrap();
        match res {
            Expr::Aggregate(selector) => println!("{:#?}", selector),
            _ => {
                panic!("not an aggregate");
            }
        };
    }
}
