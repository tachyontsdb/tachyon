use std::{
    collections::HashSet,
    fs::{self, ReadDir},
    hash::Hash,
    path::{Path, PathBuf},
    result,
};

use promql_parser::label::Matchers;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::Timestamp;

#[derive(Serialize, Deserialize, Debug)]
struct IdsEntry {
    ids: HashSet<Uuid>,
}

trait IndexerStore {
    fn insert_new_id(&self, stream: &str, matchers: &Matchers) -> Uuid;
    fn insert_new_file(&self, id: &Uuid, file: &Path, start: &Timestamp, end: &Timestamp);
    fn get_ids(&self, conn: &Connection, name: &str, value: &str) -> IdsEntry;
    fn get_stream_and_matcher_ids(
        &self,
        stream_ids: &str,
        matchers: &Matchers,
    ) -> Vec<HashSet<Uuid>>;
    fn get_files_for_stream_ids(
        &self,
        streams: &HashSet<Uuid>,
        start: &Timestamp,
        end: &Timestamp,
    ) -> Vec<PathBuf>;
}

struct SQLiteIndexerStore {
    db_path: PathBuf,
}

impl SQLiteIndexerStore {
    fn new(root_dir: &Path) -> Self {
        Self {
            db_path: root_dir.join("indexer.sqlite"),
        }
    }
}

impl IndexerStore for SQLiteIndexerStore {
    fn insert_new_id(&self, stream: &str, matchers: &Matchers) -> Uuid {
        let new_id = Uuid::new_v4();

        let mut conn = Connection::open(&self.db_path).unwrap();
        let mut stmt = conn
            .prepare("INSERT INTO stream_to_ids (name, value, ids) VALUES (?, ?, ?)")
            .unwrap();

        // update and insert for stream
        let mut stream_ids: IdsEntry = self.get_ids(&conn, "__name", stream);
        stream_ids.ids.insert(new_id);
        let stream_id_str = serde_json::to_string(&stream_ids).unwrap();
        stmt.execute(["__name", stream, &stream_id_str]);

        // update and insert for matchers
        for matcher in &matchers.matchers {
            let mut matcher_ids: IdsEntry = self.get_ids(&conn, &matcher.name, &matcher.value);
            matcher_ids.ids.insert(new_id);
            let matcher_id_str = serde_json::to_string(&matcher_ids).unwrap();
            stmt.execute([
                matcher.name.to_owned(),
                matcher.value.to_owned(),
                matcher_id_str,
            ]);
        }

        new_id
    }

    fn insert_new_file(&self, id: &Uuid, file: &Path, start: &Timestamp, end: &Timestamp) {
        let mut conn = Connection::open(&self.db_path).unwrap();
        conn.execute(
            "INSERT INTO id_to_files (id, filename, start, end) VALUES (?, ?, ?, ?)",
            (id, file.to_str(), start, end),
        );
    }

    fn get_ids(&self, conn: &Connection, name: &str, value: &str) -> IdsEntry {
        let result = conn.query_row(
            "SELECT ids FROM stream_to_ids WHERE name = ? AND value = ?",
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

    fn get_stream_and_matcher_ids(&self, stream: &str, matchers: &Matchers) -> Vec<HashSet<Uuid>> {
        let mut ids: Vec<HashSet<Uuid>> = vec![];
        let conn = Connection::open(&self.db_path).unwrap();

        ids.push(self.get_ids(&conn, "__name", stream).ids);
        for matcher in &matchers.matchers {
            ids.push(self.get_ids(&conn, &matcher.name, &matcher.value).ids);
        }

        ids
    }

    fn get_files_for_stream_ids(
        &self,
        stream_ids: &HashSet<Uuid>,
        start: &Timestamp,
        end: &Timestamp,
    ) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = Vec::new();

        let mut conn = Connection::open(&self.db_path).unwrap();
        let mut stmt = conn
            .prepare("SELECT filename FROM id_to_files WHERE id = ? AND start BETWEEN ? AND ? OR end BETWEEN ? AND ?")
            .unwrap();

        for stream_id in stream_ids {
            if let Ok(rows) = stmt.query((stream_id, start, end, start, end)) {
                let mapped_rows = rows.mapped(|row| row.get::<usize, String>(0));

                for row in mapped_rows {
                    paths.push(PathBuf::from(row.unwrap()));
                }
            }
        }

        paths
    }
}

struct Indexer {
    store: Box<dyn IndexerStore>,
    root_dir: PathBuf,
}

impl Indexer {
    fn new(root_dir: PathBuf) -> Self {
        Self {
            store: Box::new(SQLiteIndexerStore::new(&root_dir)),
            root_dir,
        }
    }

    fn insert_new_id(&self, stream: &str, matchers: &Matchers) -> Uuid {
        self.store.insert_new_id(stream, matchers)
    }

    fn insert_new_file(&self, id: &Uuid, file: &Path, start: &Timestamp, end: &Timestamp) {
        self.store.insert_new_file(id, file, start, end);
    }

    fn get_intersecting_ids(&self, id_lists: &[HashSet<Uuid>]) -> HashSet<Uuid> {
        let mut intersection: HashSet<Uuid> = HashSet::new();

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
        stream: &str,
        matchers: &Matchers,
        start: &Timestamp,
        end: &Timestamp,
    ) -> Vec<PathBuf> {
        let id_lists = self.store.get_stream_and_matcher_ids(stream, matchers);
        let intersecting_ids = self.get_intersecting_ids(&id_lists);
        self
            .store
            .get_files_for_stream_ids(&intersecting_ids, start, end)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use rusqlite::Connection;
    use uuid::Uuid;

    use super::Indexer;

    #[test]
    fn test_intersection() {
        let indexer = Indexer::new(PathBuf::from(""));

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();
        let uuid4 = Uuid::new_v4();
        let uuid5 = Uuid::new_v4();

        let hs1 = HashSet::from([uuid1, uuid2, uuid3, uuid4, uuid5]);
        let hs2 = HashSet::from([uuid1, uuid3, uuid5]);
        let hs3 = HashSet::from([uuid1, uuid5]);

        let intersect = indexer.get_intersecting_ids(&Vec::from([hs1, hs2, hs3]));

        assert_eq!(intersect, HashSet::from([uuid1, uuid5]));
    }

    #[test]
    fn test_get_required_files() {
        // SQLite Setup
        let mut conn = Connection::open("./tmp/indexer.sqlite").unwrap();

        conn.execute("DROP TABLE if exists stream_to_ids", ())
            .unwrap();
        conn.execute(
            "
                CREATE TABLE stream_to_ids (
                    name TEXT,
                    value TEXT,
                    ids TEXT
                )
                ",
            (),
        )
        .unwrap();

        conn.execute("DROP TABLE if exists id_to_files", ())
            .unwrap();
        conn.execute(
            "
                CREATE TABLE id_to_files (
                    id TEXT,
                    filename TEXT,
                    start INTEGER,
                    end INTEGER
                )
                ",
            (),
        )
        .unwrap();

        // Seeding indexer storage
        let indexer = Indexer::new(PathBuf::from("./tmp/"));
        let stream = "https";
        let matchers = Matchers::new(vec![
            Matcher::new(MatchOp::Equal, "app", "dummy"),
            Matcher::new(MatchOp::Equal, "service", "backend"),
        ]);

        let id = indexer.insert_new_id(stream, &matchers);

        let file1 = PathBuf::from(format!("./tmp/{}/file1.ty", id));
        indexer.insert_new_file(&id, &file1, &1, &3);

        let file2 = PathBuf::from(format!("./tmp/{}/file2.ty", id));
        indexer.insert_new_file(&id, &file2, &3, &5);

        let file3 = PathBuf::from(format!("./tmp/{}/file3.ty", id));
        indexer.insert_new_file(&id, &file3, &5, &7);

        // Query indexer storage
        let filenames = indexer.get_required_files(stream, &matchers, &2, &4);
        assert_eq!(filenames, Vec::from([file1, file2]));
    }
}
