use std::{
    collections::{HashMap, HashSet},
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

// SQLite Store Constants
const SQLITE_DB_NAME: &str = "indexer.sqlite";
const SQLITE_STREAM_TO_IDS_TABLE: &str = "stream_to_ids";
const SQLITE_ID_TO_FILENAME_TABLE: &str = "id_to_file";

#[derive(Serialize, Deserialize, Debug)]
struct IdsEntry {
    ids: HashSet<Uuid>,
}

trait IndexerStore {
    fn create_store(&self);
    fn drop_store(&self);
    fn insert_new_id(&mut self, stream: &str, matchers: &Matchers) -> Uuid;
    fn insert_new_file(&self, id: &Uuid, file: &Path, start: &Timestamp, end: &Timestamp);
    fn get_ids(&self, name: &str, value: &str) -> IdsEntry;
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
    conn: Connection,
}

impl SQLiteIndexerStore {
    fn new(root_dir: &Path) -> Self {
        let db_path = root_dir.join(SQLITE_DB_NAME);
        Self {
            conn: Connection::open(&db_path).unwrap(),
            db_path,
        }
    }
}

impl IndexerStore for SQLiteIndexerStore {
    fn create_store(&self) {
        self.conn
            .execute(
                &format!(
                    "
                CREATE TABLE {} (
                    name TEXT,
                    value TEXT,
                    ids TEXT
                )
                ",
                    SQLITE_STREAM_TO_IDS_TABLE
                ),
                (),
            )
            .unwrap();

        self.conn
            .execute(
                &format!(
                    "
                CREATE TABLE {} (
                    id INTEGER,
                    filename TEXT,
                    start INTEGER,
                    end INTEGER
                )
                ",
                    SQLITE_ID_TO_FILENAME_TABLE
                ),
                (),
            )
            .unwrap();
    }

    fn drop_store(&self) {
        self.conn
            .execute(
                &format!("DROP TABLE if exists {}", SQLITE_STREAM_TO_IDS_TABLE),
                (),
            )
            .unwrap();

        self.conn
            .execute(
                &format!("DROP TABLE if exists {}", SQLITE_ID_TO_FILENAME_TABLE),
                (),
            )
            .unwrap();
    }

    fn insert_new_id(&mut self, stream: &str, matchers: &Matchers) -> Uuid {
        let new_id = Uuid::new_v4();

        // get old ids and add new one
        let mut stream_ids = self.get_ids("__name", stream);
        stream_ids.ids.insert(new_id);

        let mut matcher_ids_map: HashMap<String, IdsEntry> = Default::default();
        for matcher in &matchers.matchers {
            let mut matcher_ids = self.get_ids(&matcher.name, &matcher.value);
            matcher_ids.ids.insert(new_id);

            matcher_ids_map.insert(matcher.name.to_owned(), matcher_ids);
        }

        // commit changes to db
        let mut transaction = self.conn.transaction().unwrap();
        let mut stmt = transaction
            .prepare(&format!(
                "INSERT INTO {} (name, value, ids) VALUES (?, ?, ?)",
                SQLITE_STREAM_TO_IDS_TABLE
            ))
            .unwrap();

        let stream_id_str = serde_json::to_string(&stream_ids).unwrap();
        stmt.execute(["__name", stream, &stream_id_str]);

        for matcher in &matchers.matchers {
            let matcher_id_str =
                serde_json::to_string(&matcher_ids_map.get(&matcher.name)).unwrap();
            stmt.execute([&matcher.name, &matcher.value, &matcher_id_str]);
        }

        drop(stmt);
        transaction.commit().unwrap();

        new_id
    }

    fn insert_new_file(&self, id: &Uuid, file: &Path, start: &Timestamp, end: &Timestamp) {
        self.conn.execute(
            &format!(
                "INSERT INTO {} (id, filename, start, end) VALUES (?, ?, ?, ?)",
                SQLITE_ID_TO_FILENAME_TABLE
            ),
            (id, file.to_str(), start, end),
        );
    }

    fn get_ids(&self, name: &str, value: &str) -> IdsEntry {
        let result = self.conn.query_row(
            &format!(
                "SELECT ids FROM {} WHERE name = ? AND value = ?",
                SQLITE_STREAM_TO_IDS_TABLE
            ),
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

        ids.push(self.get_ids("__name", stream).ids);
        for matcher in &matchers.matchers {
            ids.push(self.get_ids(&matcher.name, &matcher.value).ids);
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

        let mut stmt = self.conn
            .prepare(&format!("SELECT filename FROM {} WHERE id = ? AND start BETWEEN ? AND ? OR end BETWEEN ? AND ?", SQLITE_ID_TO_FILENAME_TABLE))
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

    fn create_store(root_dir: PathBuf) {
        SQLiteIndexerStore::new(&root_dir).create_store();
    }

    fn drop_store(root_dir: PathBuf) {
        SQLiteIndexerStore::new(&root_dir).drop_store();
    }

    fn insert_new_id(&mut self, stream: &str, matchers: &Matchers) -> Uuid {
        self.store.insert_new_id(stream, matchers)
    }

    fn insert_new_file(&self, id: &Uuid, file: &Path, start: &Timestamp, end: &Timestamp) {
        self.store.insert_new_file(id, file, start, end);
    }

    fn get_intersecting_ids(&self, id_lists: &mut [HashSet<Uuid>]) -> HashSet<Uuid> {
        let mut intersection: HashSet<Uuid> = HashSet::new();

        if !id_lists.is_empty() {
            for i in 0..id_lists.len() {
                if id_lists[0].len() > id_lists[i].len() {
                    id_lists.swap(0, i);
                }
            }

            intersection = id_lists[0]
                .iter()
                .filter(|k| id_lists[1..].iter().all(|s| s.contains(k)))
                .cloned()
                .collect();
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
        let mut id_lists = self.store.get_stream_and_matcher_ids(stream, matchers);
        let intersecting_ids = self.get_intersecting_ids(&mut id_lists);
        self.store
            .get_files_for_stream_ids(&intersecting_ids, start, end)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use rusqlite::Connection;
    use uuid::Uuid;

    use crate::{
        query::indexer::{SQLITE_DB_NAME, SQLITE_ID_TO_FILENAME_TABLE, SQLITE_STREAM_TO_IDS_TABLE},
        utils::test_utils::{set_up_dirs, set_up_files},
    };

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

        let intersect = indexer.get_intersecting_ids(&mut Vec::from([hs1, hs2, hs3]));

        assert_eq!(intersect, HashSet::from([uuid1, uuid5]));
    }

    #[test]
    fn test_get_required_files() {
        set_up_dirs!(dirs, "db");

        // Seeding indexer storage
        let mut indexer = Indexer::new(dirs[0].clone());
        Indexer::drop_store(dirs[0].clone());
        Indexer::create_store(dirs[0].clone());
        let stream = "https";
        let matchers = Matchers::new(vec![
            Matcher::new(MatchOp::Equal, "app", "dummy"),
            Matcher::new(MatchOp::Equal, "service", "backend"),
        ]);

        let id = indexer.insert_new_id(stream, &matchers);

        let file1 = PathBuf::from(format!("{}/{}/file1.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(&id, &file1, &1, &3);

        let file2 = PathBuf::from(format!("{}/{}/file2.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(&id, &file2, &3, &5);

        let file3 = PathBuf::from(format!("{}/{}/file3.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(&id, &file3, &5, &7);

        // Query indexer storage
        let filenames = indexer.get_required_files(stream, &matchers, &2, &4);
        assert_eq!(filenames, Vec::from([file1, file2]));
    }
}
