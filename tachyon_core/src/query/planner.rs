use crate::error::QueryErr;
use crate::execution::node::{
    AggregateNode, AggregateType, ArithmeticOp, BinaryOp, BinaryOpNode, ComparisonOp, GetKNode,
    GetKType, NumberLiteralNode, TNode, VectorSelectNode,
};
use crate::storage::file::ScanHint;
use crate::{Connection, Timestamp, ValueType};
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

    pub fn plan(&mut self, conn: &mut Connection) -> Result<TNode, QueryErr> {
        self.handle_expr(self.ast, conn, ScanHint::None)
    }

    fn handle_aggregate_expr(
        &mut self,
        expr: &AggregateExpr,
        conn: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        match expr.op.id() {
            parser::token::T_SUM
            | parser::token::T_COUNT
            | parser::token::T_MIN
            | parser::token::T_MAX => {
                let (aggregate_type, scan_hint) = match expr.op.id() {
                    parser::token::T_SUM => (AggregateType::Sum, ScanHint::Sum),
                    parser::token::T_COUNT => (AggregateType::Count, ScanHint::Count),
                    parser::token::T_MIN => (AggregateType::Min, ScanHint::Min),
                    parser::token::T_MAX => (AggregateType::Max, ScanHint::Max),
                    _ => unreachable!(),
                };
                Ok(TNode::Aggregate(AggregateNode::new(
                    aggregate_type,
                    Box::new(self.handle_expr(&expr.expr, conn, scan_hint)?),
                    None,
                )))
            }
            parser::token::T_AVG => Ok(TNode::Aggregate(AggregateNode::new(
                AggregateType::Average,
                Box::new(self.handle_expr(&expr.expr, conn, ScanHint::Sum)?),
                Some(Box::new(self.handle_expr(
                    &expr.expr,
                    conn,
                    ScanHint::Count,
                )?)),
            ))),
            parser::token::T_BOTTOMK | parser::token::T_TOPK => {
                let child = Box::new(self.handle_expr(&expr.expr, conn, ScanHint::None)?);

                if let Some(param_expr) = &expr.param {
                    let param = Box::new(self.handle_expr(param_expr, conn, ScanHint::None)?);
                    Ok(TNode::GetK(GetKNode::new(
                        conn,
                        match expr.op.id() {
                            parser::token::T_BOTTOMK => GetKType::Bottom,
                            parser::token::T_TOPK => GetKType::Top,
                            _ => unreachable!(),
                        },
                        child,
                        param,
                    )))
                } else {
                    Err(QueryErr::QuerySyntaxErr)
                }
            }
            _ => Err(QueryErr::QuerySyntaxErr),
        }
    }

    fn handle_unary_expr(&mut self, _: &UnaryExpr, _: &mut Connection) -> Result<TNode, QueryErr> {
        Err(QueryErr::UnsupportedErr {
            expr_type: "Unary".to_string(),
        })
    }

    fn handle_binary_expr(
        &mut self,
        expr: &BinaryExpr,
        conn: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        let op = match expr.op.id() {
            parser::token::T_ADD => Ok(BinaryOp::Arithmetic(ArithmeticOp::Add)),
            parser::token::T_SUB => Ok(BinaryOp::Arithmetic(ArithmeticOp::Subtract)),
            parser::token::T_MUL => Ok(BinaryOp::Arithmetic(ArithmeticOp::Multiply)),
            parser::token::T_DIV => Ok(BinaryOp::Arithmetic(ArithmeticOp::Divide)),
            parser::token::T_MOD => Ok(BinaryOp::Arithmetic(ArithmeticOp::Modulo)),
            parser::token::T_EQLC => Ok(BinaryOp::Comparison(ComparisonOp::Equal)),
            parser::token::T_NEQ => Ok(BinaryOp::Comparison(ComparisonOp::NotEqual)),
            parser::token::T_GTR => Ok(BinaryOp::Comparison(ComparisonOp::Greater)),
            parser::token::T_LSS => Ok(BinaryOp::Comparison(ComparisonOp::Less)),
            parser::token::T_GTE => Ok(BinaryOp::Comparison(ComparisonOp::GreaterEqual)),
            parser::token::T_LTE => Ok(BinaryOp::Comparison(ComparisonOp::LessEqual)),
            _ => Err(QueryErr::QuerySyntaxErr),
        };

        Ok(TNode::BinaryOp(BinaryOpNode::new(
            op?,
            Box::new(self.handle_expr(&expr.lhs, conn, ScanHint::None)?),
            Box::new(self.handle_expr(&expr.rhs, conn, ScanHint::None)?),
        )))
    }

    fn handle_paren_expr(
        &mut self,
        expr: &ParenExpr,
        conn: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        self.handle_expr(&expr.expr, conn, ScanHint::None)
    }

    fn handle_subquery_expr(
        &mut self,
        _: &SubqueryExpr,
        _: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        Err(QueryErr::UnsupportedErr {
            expr_type: "Subquery".to_string(),
        })
    }

    fn handle_number_literal_expr(
        &mut self,
        expr: &NumberLiteral,
        _: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        Ok(TNode::NumberLiteral(NumberLiteralNode::new(
            ValueType::Float64,
            expr.val.into(),
        )))
    }

    fn handle_string_literal_expr(
        &mut self,
        _: &StringLiteral,
        _: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        Err(QueryErr::UnsupportedErr {
            expr_type: "String Literal".to_string(),
        })
    }

    fn handle_vector_selector_expr(
        &mut self,
        expr: &VectorSelector,
        conn: &mut Connection,
        hint: ScanHint,
    ) -> Result<TNode, QueryErr> {
        let start_opt = if expr.at.is_some() {
            // SAFETY: expr.at is Some from above, unwrapping is safe
            let mut at_res = match expr.at.as_ref().unwrap() {
                parser::AtModifier::Start => 0,
                parser::AtModifier::End => u64::MAX,
                parser::AtModifier::At(t) => t.elapsed()?.as_millis() as u64,
            };

            if expr.offset.is_some() {
                // SAFETY: expr.offset is Some from above, unwrapping is safe
                at_res = match expr.offset.as_ref().unwrap() {
                    parser::Offset::Pos(t) => at_res.saturating_add(t.as_millis() as u64),
                    parser::Offset::Neg(t) => at_res.saturating_sub(t.as_millis() as u64),
                }
            }
            Some(at_res)
        } else {
            self.start
        };

        if let Some(start) = start_opt {
            if let Some(end) = self.end {
                if let Some(name) = &expr.name {
                    Ok(TNode::VectorSelect(VectorSelectNode::new(
                        conn,
                        name.to_string(),
                        expr.matchers.clone(),
                        start,
                        end,
                        hint,
                    )?))
                } else {
                    Err(QueryErr::QuerySyntaxErr)
                }
            } else {
                Err(QueryErr::StartEndTimeErr {
                    start_or_end: "end".to_string(),
                })
            }
        } else {
            Err(QueryErr::StartEndTimeErr {
                start_or_end: "start".to_string(),
            })
        }
    }

    fn handle_matrix_selector_expr(
        &mut self,
        _: &MatrixSelector,
        _: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        Err(QueryErr::UnsupportedErr {
            expr_type: ("Matrix".to_string()),
        })
    }

    fn handle_call_expr(&mut self, _: &Call, _: &mut Connection) -> Result<TNode, QueryErr> {
        Err(QueryErr::UnsupportedErr {
            expr_type: ("Call".to_string()),
        })
    }

    fn handle_extension_expr(
        &mut self,
        _: &Extension,
        _: &mut Connection,
    ) -> Result<TNode, QueryErr> {
        Err(QueryErr::UnsupportedErr {
            expr_type: ("Extension".to_string()),
        })
    }

    fn handle_expr(
        &mut self,
        expr: &Expr,
        conn: &mut Connection,
        hint: ScanHint,
    ) -> Result<TNode, QueryErr> {
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
