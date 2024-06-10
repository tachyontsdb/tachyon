use promql_parser::parser::{self, AggregateExpr, BinaryExpr, Call, Expr, Extension, MatrixSelector, NumberLiteral, ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr, VectorSelector};

use crate::common::Value;

fn handle_aggregate_expr(expr: &AggregateExpr) -> Result<Vec<Value>, &'static str> {
    return Err("Aggregate expressions currently not supported.");
}

fn handle_unary_expr(expr: &UnaryExpr) -> Result<Vec<Value>, &'static str> {
    let result = exec_expr(&expr.expr);
    todo!()
}

fn handle_binary_expr(expr: &BinaryExpr) -> Result<Vec<Value>, &'static str> {
    return Err("Binary expressions currently not supported.");
}

fn handle_paren_expr(expr: &ParenExpr) -> Result<Vec<Value>, &'static str> {
    return exec_expr(&expr.expr);
}

fn handle_subquery_expr(expr: &SubqueryExpr) -> Result<Vec<Value>, &'static str> {
    return Err("Subquery expressions currently not supported.");
}

fn handle_number_literal_expr(expr: &NumberLiteral) -> Result<Vec<Value>, &'static str> {
    todo!()
}

fn handle_string_literal_expr(expr: &StringLiteral) -> Result<Vec<Value>, &'static str> {
    todo!()
}

fn handle_vector_selector_expr(expr: &VectorSelector) -> Result<Vec<Value>, &'static str> {
    todo!()
}

fn handle_matrix_selector_expr(expr: &MatrixSelector) -> Result<Vec<Value>, &'static str> {
    todo!()
}

fn handle_call_expr(expr: &Call) -> Result<Vec<Value>, &'static str> {
    return Err("Call expressions currently not supported.");
}

fn handle_extension_expr(expr: &Extension) -> Result<Vec<Value>, &'static str> {
    return Err("Extension expressions currently not supported.");
}

fn exec_expr(expr: &Expr) -> Result<Vec<Value>, &'static str> {
    match expr {
        Expr::Aggregate(aggregate_expr) => handle_aggregate_expr(aggregate_expr),
        Expr::Unary(unary_expr) => handle_unary_expr(unary_expr),
        Expr::Binary(binary_expr) => handle_binary_expr(binary_expr),
        Expr::Paren(paren_expr) => handle_paren_expr(paren_expr),
        Expr::Subquery(subquery_expr) => handle_subquery_expr(subquery_expr),
        Expr::NumberLiteral(number_literal_expr) => handle_number_literal_expr(number_literal_expr),
        Expr::StringLiteral(string_literal_expr) => handle_string_literal_expr(string_literal_expr),
        Expr::VectorSelector(vector_selector_expr) => handle_vector_selector_expr(vector_selector_expr),
        Expr::MatrixSelector(matrix_selector_expr) => handle_matrix_selector_expr(matrix_selector_expr),
        Expr::Call(call_expr) => handle_call_expr(call_expr),
        Expr::Extension(extension_expr) => handle_extension_expr(extension_expr),
    }
}

fn query(s: &str) {
    let expr = parser::parse(s).unwrap();
    print!("{}", expr.prettify());
    // exec_expr(&expr);
}

fn main() {
    let query_string = r#"http_requests_total{job="prometheus",group="canary"}"#;
    query(&query_string);
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query() {
        let query_string = r#"http_requests_total{job="prometheus",group="canary"}"#;
        query(&query_string);
    }
}
