use promql_parser::label::{Matcher, Matchers};
use uuid::Uuid;

use crate::{
    api::Connection,
    common::{Timestamp, Value},
    storage::file::{Cursor, ScanHint},
    utils::common::static_assert,
};

pub trait ExecutorNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        panic!("Next scalar not implemented")
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        panic!("Next vector not implemented")
    }
}

pub enum TNode {
    VectorSelect(VectorSelectNode),
    Sum(SumNode),
    Average(AverageNode),
}

impl ExecutorNode for TNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        match self {
            TNode::VectorSelect(sel) => sel.next_vector(conn),
            TNode::Sum(sum) => sum.next_vector(conn),
            TNode::Average(avg) => avg.next_vector(conn),
        }
    }
}

pub struct VectorSelectNode {
    stream_ids: Vec<Uuid>,
    stream_idx: usize,
    cursor: Cursor,
}

impl VectorSelectNode {
    pub fn new(
        conn: &mut Connection,
        name: String,
        matchers: Matchers,
        start: Timestamp,
        end: Timestamp,
    ) -> Self {
        let stream_ids: Vec<Uuid> = conn
            .indexer
            .borrow()
            .get_stream_ids(&name, &matchers)
            .into_iter()
            .collect();

        if stream_ids.is_empty() {
            panic!("No streams match selector");
        }

        let stream_id = stream_ids[0];
        println!("STREAM ID: {}, start: {}, end: {}", stream_id, start, end);
        let file_paths = conn
            .indexer
            .borrow()
            .get_required_files(&stream_id, &start, &end);

        VectorSelectNode {
            stream_ids,
            stream_idx: 0,
            cursor: Cursor::new(
                file_paths,
                start,
                end,
                conn.page_cache.clone(),
                ScanHint::None,
            )
            .unwrap(),
        }
    }
}

impl ExecutorNode for VectorSelectNode {
    fn next_vector(&mut self, conn: &mut Connection) -> Option<(Timestamp, Value)> {
        if self.cursor.is_done() {
            self.stream_idx += 1;
            // support multiple streams here
            return None;
        }
        let res = self.cursor.fetch();
        self.cursor.next();
        Some(res)
    }
}

pub struct SumNode {
    child: Box<TNode>,
}

impl ExecutorNode for SumNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        todo!()
    }
}

pub struct AverageNode {
    child: Box<TNode>,
}

impl ExecutorNode for AverageNode {
    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        todo!()
    }
}

mod test {
    use promql_parser::parser;

    #[test]
    fn example_query() {
        let stmt = r#"sum(http_requests_total)"#;
        let ast = parser::parse(stmt);
        println!("{:#?}", ast);
    }
}
