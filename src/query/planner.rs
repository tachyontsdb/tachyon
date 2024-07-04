use promql_parser::parser::{
    self, AggregateExpr, BinaryExpr, Call, Expr, Extension, MatrixSelector, NumberLiteral,
    ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr, VectorSelector,
};

use crate::common::{Timestamp, Value};
use crate::executor::{self, execute, Context, OperationCode::*};

#[derive(Debug)]
struct QueryPlanner<'a> {
    cursor_idx: u64,
    ast: &'a Expr,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ast: &'a Expr) -> Self {
        Self { cursor_idx: 0, ast }
    }

    pub fn plan(&mut self) -> Vec<u8> {
        self.handle_expr(self.ast).unwrap()
    }

    fn handle_aggregate_expr(&mut self, expr: &AggregateExpr) -> Result<Vec<u8>, &'static str> {
        Err("Aggregate expressions currently not supported.")
    }

    fn handle_unary_expr(&mut self, expr: &UnaryExpr) -> Result<Vec<u8>, &'static str> {
        let result = self.handle_expr(&expr.expr);
        todo!()
    }

    fn handle_binary_expr(&mut self, expr: &BinaryExpr) -> Result<Vec<u8>, &'static str> {
        Err("Binary expressions currently not supported.")
    }

    fn handle_paren_expr(&mut self, expr: &ParenExpr) -> Result<Vec<u8>, &'static str> {
        self.handle_expr(&expr.expr)
    }

    fn handle_subquery_expr(&mut self, expr: &SubqueryExpr) -> Result<Vec<u8>, &'static str> {
        Err("Subquery expressions currently not supported.")
    }

    fn handle_number_literal_expr(
        &mut self,
        expr: &NumberLiteral,
    ) -> Result<Vec<u8>, &'static str> {
        todo!()
    }

    fn handle_string_literal_expr(
        &mut self,
        expr: &StringLiteral,
    ) -> Result<Vec<u8>, &'static str> {
        todo!()
    }

    fn handle_vector_selector_expr(
        &mut self,
        expr: &VectorSelector,
    ) -> Result<Vec<u8>, &'static str> {
        let name = expr.name.as_ref().unwrap();

        // Create the context with the files

        let buffer = [Init, OpenRead, FetchVector, Next, Halt, OutputVector];
        let file_paths = [""];
        let cursor_idx = 0;
        // executor::execute(context, buffer);
        Ok(Vec::new())
    }

    fn handle_matrix_selector_expr(
        &mut self,
        expr: &MatrixSelector,
    ) -> Result<Vec<u8>, &'static str> {
        todo!()
    }

    fn handle_call_expr(&mut self, expr: &Call) -> Result<Vec<u8>, &'static str> {
        Err("Call expressions currently not supported.")
    }

    fn handle_extension_expr(&mut self, expr: &Extension) -> Result<Vec<u8>, &'static str> {
        Err("Extension expressions currently not supported.")
    }

    fn handle_expr(&mut self, expr: &Expr) -> Result<Vec<u8>, &'static str> {
        match expr {
            Expr::Aggregate(aggregate_expr) => self.handle_aggregate_expr(aggregate_expr),
            Expr::Unary(unary_expr) => self.handle_unary_expr(unary_expr),
            Expr::Binary(binary_expr) => self.handle_binary_expr(binary_expr),
            Expr::Paren(paren_expr) => self.handle_paren_expr(paren_expr),
            Expr::Subquery(subquery_expr) => self.handle_subquery_expr(subquery_expr),
            Expr::NumberLiteral(number_literal_expr) => {
                self.handle_number_literal_expr(number_literal_expr)
            }
            Expr::StringLiteral(string_literal_expr) => {
                self.handle_string_literal_expr(string_literal_expr)
            }
            Expr::VectorSelector(vector_selector_expr) => {
                self.handle_vector_selector_expr(vector_selector_expr)
            }
            Expr::MatrixSelector(matrix_selector_expr) => {
                self.handle_matrix_selector_expr(matrix_selector_expr)
            }
            Expr::Call(call_expr) => self.handle_call_expr(call_expr),
            Expr::Extension(extension_expr) => self.handle_extension_expr(extension_expr),
        }
    }
}

fn query(s: &str, start: Option<Timestamp>, end: Option<Timestamp>) {
    let ast = parser::parse(s).unwrap();
    let mut planner = QueryPlanner::new(&ast);
    let bytes = planner.plan();

    // execute(context, buffer);
    println!("{:#?}", planner);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query() {
        let query_string = r#"http_requests_total"#;
        let start = None;
        let end = None;
        query(query_string, start, end);
    }
}
