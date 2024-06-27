use std::{
    collections::HashSet,
    fs::{self, ReadDir},
    hash::Hash,
    path::PathBuf,
    result,
};

use promql_parser::label::Matchers;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::Timestamp;

#[derive(Serialize, Deserialize, Debug)]
struct IdsEntry {
    ids: HashSet<u128>,
}

trait IndexerStore {
    fn insert_new_id(&self, stream: &str, matchers: &Matchers);
    fn get_ids(&self, conn: &Connection, name: &str, value: &str) -> IdsEntry;
    fn get_stream_and_matcher_ids(
        &self,
        stream: &str,
        matchers: &Matchers,
    ) -> Vec<HashSet<u128>>;
}

struct SQLiteIndexerStore {
    db_path: PathBuf,
}

impl SQLiteIndexerStore {
    fn new(root_dir: &PathBuf) -> Self {
        let mut db_path = root_dir.clone();
        db_path.set_file_name("indexer.sqlite");

        Self { db_path: db_path }
    }
}

impl IndexerStore for SQLiteIndexerStore {
    fn insert_new_id(&self, stream: &str, matchers: &Matchers) {
        let new_id = Uuid::new_v4();

        let mut conn = Connection::open(&self.db_path).unwrap();
        let mut stmt = conn
            .prepare("INSERT INTO mapping (name, value, ids) VALUES (?, ?, ?)")
            .unwrap();

        // update and insert for stream
        let mut stream_ids: IdsEntry = self.get_ids(&conn, "__name", stream);
        stream_ids.ids.insert(new_id.as_u128());
        let stream_id_str = serde_json::to_string(&stream_ids).unwrap();
        stmt.execute(["__name", stream, &stream_id_str]);

        // update and insert for matchers
        for matcher in &matchers.matchers {
            let mut matcher_ids: IdsEntry = self.get_ids(&conn, &matcher.name, &matcher.value);
            matcher_ids.ids.insert(new_id.as_u128());
            let matcher_id_str = serde_json::to_string(&matcher_ids).unwrap();
            stmt.execute([
                matcher.name.to_owned(),
                matcher.value.to_owned(),
                matcher_id_str,
            ]);
        }
    }

    fn get_ids(&self, conn: &Connection, name: &str, value: &str) -> IdsEntry {
        let result = conn.query_row(
            "SELECT ids FROM mapping WHERE name = ? AND value = ?",
            [name, value],
            |row| row.get::<usize, String>(0),
        );

        match result {
            Ok(stream_ids_str) => serde_json::from_str(stream_ids_str.as_str()).unwrap(),
            Err(e) => IdsEntry {
                ids: HashSet::new(),
            },
        }
    }

    fn get_stream_and_matcher_ids(
        &self,
        stream: &str,
        matchers: &Matchers,
    ) -> Vec<HashSet<u128>> {
        let mut ids: Vec<HashSet<u128>> = vec![];

        let conn = Connection::open(&self.db_path).unwrap();

        ids.push(self.get_ids(&conn, "__name", stream).ids);

        for matcher in &matchers.matchers {
            ids.push(self.get_ids(&conn, &matcher.name, &matcher.value).ids);
        }

        ids
    }
}

struct Indexer {
    store: Box<dyn IndexerStore>,
}

impl Indexer {
    fn new(root_dir: PathBuf) -> Self {
        Self {
            store: Box::new(SQLiteIndexerStore::new(&root_dir)),
        }
    }

    fn insert_new_id(&self, stream: &str, matchers: &Matchers) {
        self.store.insert_new_id(stream, matchers);
    }

    fn get_intersecting_ids(&self, id_lists: &[HashSet<u128>]) -> HashSet<u128> {
        let mut intersection: HashSet<u128> = HashSet::new();

        if !id_lists.is_empty() {
            for id in &id_lists[0] {
                let mut is_in_others = true;

                for other in &id_lists[1..] {
                    if !other.contains(id) {
                        is_in_others = false;
                        break;
                    }
                }

                if is_in_others {
                    intersection.insert(*id);
                }
            }
        }

        intersection
    }

    fn get_required_files(
        &self,
        stream: String,
        matchers: Matchers,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<String>, &'static str> {
        let id_lists = self.store.get_stream_and_matcher_ids(&stream, &matchers);
        let intersecting_ids = self.get_intersecting_ids(&id_lists);

        for id in intersecting_ids {
            println!("{}", id);
        }
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, hash::Hash, path::PathBuf};

    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use rusqlite::Connection;

    use super::Indexer;

    #[test]
    fn test_intersection() {
        let indexer = Indexer::new(PathBuf::from("root_dir/"));

        let hs1 = HashSet::from([1, 2, 3, 4, 5]);
        let hs2 = HashSet::from([1, 3, 5]);
        let hs3 = HashSet::from([1, 5]);

        let intersect = indexer.get_intersecting_ids(&Vec::from([hs1, hs2, hs3]));

        assert_eq!(intersect, HashSet::from([1, 5]));
    }

    #[test]
    fn test_get_stream_and_matcher_ids() {
        let mut conn = Connection::open("./tmp/indexer.sqlite").unwrap();

        conn.execute(
            "
            CREATE TABLE if not exists mapping (
                name TEXT,
                value TEXT,
                ids TEXT
            )
            ",
            (),
        )
        .unwrap();

        let indexer = Indexer::new(PathBuf::from("tmp/"));
        let matchers = Matchers::new(vec![
            Matcher::new(MatchOp::Equal, "app", "dummy"),
            Matcher::new(MatchOp::Equal, "service", "backend"),
        ]);

        indexer.insert_new_id("https", &matchers);
        let ids = indexer
            .store
            .get_stream_and_matcher_ids("https", &matchers);

        println!("{:?}", ids);
    }
}
