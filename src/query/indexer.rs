use std::{collections::HashSet, fs::{self, ReadDir}, hash::Hash, result};

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
    fn insert_new_id(&self, root_dir: &String, stream: &String, matchers: &Matchers);
    fn get_ids(&self, conn: &Connection, name: &String, value: &String) -> IdsEntry;
    fn get_stream_and_matcher_ids(&self, root_dir: &String, stream: &String, matchers: &Matchers) -> Vec<HashSet<u128>>;
}

struct SQLiteIndexerStore {}

impl IndexerStore for SQLiteIndexerStore {
    fn insert_new_id(&self, root_dir: &String, stream: &String, matchers: &Matchers) {
        let new_id = Uuid::new_v4();

        let mut conn: Connection = Connection::open(format!("{}/indexer.sqlite", root_dir)).unwrap();
        let mut stmt = conn.prepare("INSERT INTO mapping (name, value, ids) VALUES (?, ?, ?)").unwrap();

        // update and insert for stream
        let mut stream_ids: IdsEntry = self.get_ids(&conn, &"__name".to_owned(), &stream);
        stream_ids.ids.insert(new_id.as_u128());
        let stream_id_str = serde_json::to_string(&stream_ids).unwrap();
        stmt.execute(["__name", stream, &stream_id_str]);

        // update and insert for matchers
        for matcher in &matchers.matchers {
            let mut matcher_ids: IdsEntry = self.get_ids(&conn, &matcher.name, &matcher.value);
            matcher_ids.ids.insert(new_id.as_u128());
            let matcher_id_str = serde_json::to_string(&matcher_ids).unwrap();
            stmt.execute([matcher.name.to_owned(), matcher.value.to_owned(), matcher_id_str]);
        }
    }

    fn get_ids(&self, conn: &Connection, name: &String, value: &String) -> IdsEntry {
        let result = conn.query_row("SELECT ids FROM mapping WHERE name = ? AND value = ?", [name, value], |row| row.get::<usize, String>(0).map(|s| s));

        match result {
            Ok(stream_ids_str) => { 
                serde_json::from_str(stream_ids_str.as_str()).unwrap()
            },
            Err(e) => {
                IdsEntry{ ids: HashSet::new() }
            }
        }
    }

    fn get_stream_and_matcher_ids(&self, root_dir: &String, stream: &String, matchers: &Matchers) -> Vec<HashSet<u128>> {
        let mut ids: Vec<HashSet<u128>> = vec![];

        let conn: Connection = Connection::open(format!("{}/indexer.sqlite", root_dir)).unwrap();
        
        ids.push(self.get_ids(&conn, &"__name".to_owned(), &stream).ids);

        for matcher in &matchers.matchers {
            ids.push(self.get_ids(&conn, &matcher.name, &matcher.value).ids);
        }

        return ids;
    }
}

struct Indexer {
    root_dir: String,
    store: Box<dyn IndexerStore>
}

impl Indexer {
    fn new(root_dir: String) -> Self {
        Self {
            root_dir: root_dir,
            store: Box::new(SQLiteIndexerStore{})
        }
    }

    fn insert_new_id(&self, stream: &String, matchers: &Matchers) {
        self.store.insert_new_id(&self.root_dir, stream, matchers);
    }

    fn get_intersecting_ids(&self, id_lists: &Vec<HashSet<u128>>) -> HashSet<u128> {
        let mut intersection: HashSet<u128> = HashSet::new();

        if !id_lists.is_empty() {
            for id in &id_lists[0] {
                let mut is_in_others = true;
    
                for other in &id_lists[1..] {
                    if other.contains(&id) {
                        is_in_others = false;
                        break;
                    }
                }
    
                if is_in_others {
                    intersection.insert(*id);
                }
            }
        }

        return intersection;
    }

    fn get_required_files(&self, stream: String, matchers: Matchers, start: Timestamp, end: Timestamp) -> Result<Vec<String>, &'static str> {
        let id_lists = self.store.get_stream_and_matcher_ids(&self.root_dir, &stream, &matchers);
        let intersecting_ids = self.get_intersecting_ids(&id_lists);

        for id in intersecting_ids {
            println!("{}", id);

        }
        todo!()
    }    
}


#[cfg(test)]
mod tests {
    use std::{collections::HashSet, hash::Hash};

    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use rusqlite::Connection;

    use super::Indexer;

    #[test]
    fn test_intersection() {
        let indexer = Indexer::new("root_dir".to_string());

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

        let indexer = Indexer::new("tmp".to_string());
        let matchers = Matchers::new(vec![Matcher::new(MatchOp::Equal, "app", "dummy"), Matcher::new(MatchOp::Equal, "service", "backend")]);

        indexer.insert_new_id(&"https".to_owned(), &matchers);
        let ids = indexer.store.get_stream_and_matcher_ids(&"tmp".to_owned(), &"https".to_owned(), &matchers);

        println!("{:?}", ids);
    }
}
