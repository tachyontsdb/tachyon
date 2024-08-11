use crate::{Timestamp, ValueType};
use promql_parser::label::Matchers;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use uuid::Uuid;

// SQLite Store Constants
const SQLITE_DB_NAME: &'static str = "indexer.sqlite";
const SQLITE_STREAM_TO_IDS_TABLE: &'static str = "stream_to_ids";
const SQLITE_ID_TO_FILENAME_TABLE: &'static str = "id_to_file";
const SQLITE_ID_TO_VALUE_TYPE_TABLE: &'static str = "id_to_value_type";

#[derive(Serialize, Deserialize, Debug)]
pub struct IdsEntry {
    ids: HashSet<Uuid>,
}

trait IndexerStore {
    fn create_store(&mut self);
    fn drop_store(&mut self);
    fn insert_new_id(&mut self, stream: &str, matchers: &Matchers, value_type: ValueType) -> Uuid;
    fn insert_new_file(&self, id: Uuid, file: &Path, start: Timestamp, end: Timestamp);
    fn get_ids(&self, name: &str, value: &str) -> IdsEntry;
    fn get_stream_and_matcher_ids(&self, stream: &str, matchers: &Matchers) -> Vec<HashSet<Uuid>>;
    fn get_files_for_stream_id(
        &self,
        stream_id: Uuid,
        start: Timestamp,
        end: Timestamp,
    ) -> Vec<PathBuf>;
    fn get_value_type_for_stream_id(&self, stream_id: Uuid) -> Option<ValueType>;
}

struct SQLiteIndexerStore {
    db_path: PathBuf,
    conn: Connection,
}

impl SQLiteIndexerStore {
    pub fn new(root_dir: impl AsRef<Path>) -> Self {
        let db_path = root_dir.as_ref().join(SQLITE_DB_NAME);

        Self {
            conn: Connection::open(&db_path).unwrap(),
            db_path,
        }
    }
}

impl IndexerStore for SQLiteIndexerStore {
    fn create_store(&mut self) {
        let transaction = self.conn.transaction().unwrap();

        transaction
            .execute(
                &format!(
                    "
                        CREATE TABLE IF NOT EXISTS {} (
                            name TEXT,
                            value TEXT,
                            ids TEXT,
                            PRIMARY KEY (name, value)
                        )
                    ",
                    SQLITE_STREAM_TO_IDS_TABLE
                ),
                (),
            )
            .unwrap();

        transaction
            .execute(
                &format!(
                    "
                        CREATE TABLE IF NOT EXISTS {} (
                            id INTEGER,
                            filename TEXT,
                            start INTEGER,
                            end INTEGER,
                            PRIMARY KEY (id, filename)
                        )
                    ",
                    SQLITE_ID_TO_FILENAME_TABLE
                ),
                (),
            )
            .unwrap();

        transaction
            .execute(
                &format!(
                    "
                        CREATE TABLE IF NOT EXISTS {} (
                            id TEXT,
                            value_type INTEGER,
                            PRIMARY KEY (id)
                        )
                    ",
                    SQLITE_ID_TO_VALUE_TYPE_TABLE
                ),
                (),
            )
            .unwrap();

        transaction.commit().unwrap();
    }

    fn drop_store(&mut self) {
        let transaction = self.conn.transaction().unwrap();

        transaction
            .execute(
                &format!("DROP TABLE IF EXISTS {}", SQLITE_STREAM_TO_IDS_TABLE),
                (),
            )
            .unwrap();

        transaction
            .execute(
                &format!("DROP TABLE IF EXISTS {}", SQLITE_ID_TO_FILENAME_TABLE),
                (),
            )
            .unwrap();

        transaction.commit().unwrap();
    }

    fn insert_new_id(&mut self, stream: &str, matchers: &Matchers, value_type: ValueType) -> Uuid {
        let new_id = Uuid::new_v4();

        // Get old ids and add new one
        let mut stream_ids = self.get_ids("__name", stream);
        stream_ids.ids.insert(new_id);

        let mut matcher_ids_map = HashMap::<String, IdsEntry>::default();
        for matcher in &matchers.matchers {
            let mut matcher_ids = self.get_ids(&matcher.name, &matcher.value);
            matcher_ids.ids.insert(new_id);

            matcher_ids_map.insert(matcher.name.to_owned(), matcher_ids);
        }

        // Commit changes to db
        let transaction = self.conn.transaction().unwrap();

        let mut stmt = transaction
            .prepare(&format!(
                "INSERT OR REPLACE INTO {} (name, value, ids) VALUES (?, ?, ?)",
                SQLITE_STREAM_TO_IDS_TABLE
            ))
            .unwrap();

        let stream_id_str = serde_json::to_string(&stream_ids).unwrap();
        stmt.execute(["__name", stream, &stream_id_str]).unwrap();

        for matcher in &matchers.matchers {
            let matcher_id_str =
                serde_json::to_string(&matcher_ids_map.get(&matcher.name)).unwrap();
            stmt.execute([&matcher.name, &matcher.value, &matcher_id_str])
                .unwrap();
        }

        transaction
            .execute(
                &format!(
                    "INSERT INTO {} (id, value_type) VALUES (?, ?)",
                    SQLITE_ID_TO_VALUE_TYPE_TABLE
                ),
                (serde_json::to_string(&new_id).unwrap(), value_type as u8),
            )
            .unwrap();

        drop(stmt);
        transaction.commit().unwrap();

        new_id
    }

    fn insert_new_file(&self, id: Uuid, file: &Path, start: Timestamp, end: Timestamp) {
        self.conn
            .execute(
                &format!(
                    "INSERT INTO {} (id, filename, start, end) VALUES (?, ?, ?, ?)",
                    SQLITE_ID_TO_FILENAME_TABLE
                ),
                (id, file.to_str(), start, end),
            )
            .unwrap();
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
            Err(_) => IdsEntry {
                ids: HashSet::new(),
            },
        }
    }

    fn get_stream_and_matcher_ids(&self, stream: &str, matchers: &Matchers) -> Vec<HashSet<Uuid>> {
        let mut ids = Vec::<HashSet<Uuid>>::new();

        ids.push(self.get_ids("__name", stream).ids);
        for matcher in &matchers.matchers {
            ids.push(self.get_ids(&matcher.name, &matcher.value).ids);
        }

        ids
    }

    fn get_files_for_stream_id(
        &self,
        stream_id: Uuid,
        start: Timestamp,
        end: Timestamp,
    ) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = Vec::new();

        let mut stmt = self
            .conn
            .prepare(&format!(
                "SELECT filename FROM {} WHERE id = ? AND ? <= end AND ? >= start",
                SQLITE_ID_TO_FILENAME_TABLE
            ))
            .unwrap();

        let mapped_rows = stmt
            .query((stream_id, start, end))
            .unwrap()
            .mapped(|row| row.get::<usize, String>(0));

        for row in mapped_rows {
            paths.push(PathBuf::from(row.unwrap()));
        }

        paths
    }

    fn get_value_type_for_stream_id(&self, stream_id: Uuid) -> Option<ValueType> {
        self.conn
            .query_row(
                &format!(
                    "SELECT value_type FROM {} WHERE id = ?",
                    SQLITE_ID_TO_VALUE_TYPE_TABLE
                ),
                [serde_json::to_string(&stream_id).unwrap()],
                |row| row.get(0),
            )
            .map_or_else(
                |_| None,
                |value_type: u8| Some(value_type.try_into().unwrap()),
            )
    }
}

pub struct Indexer {
    store: Box<dyn IndexerStore>,
    root_dir: PathBuf,
}

impl Indexer {
    pub fn new(root_dir: impl AsRef<Path>) -> Self {
        Self {
            store: Box::new(SQLiteIndexerStore::new(&root_dir)),
            root_dir: root_dir.as_ref().to_path_buf(),
        }
    }

    pub fn create_store(&mut self) {
        self.store.create_store();
    }

    pub fn drop_store(&mut self) {
        self.store.drop_store();
    }

    pub fn insert_new_id(
        &mut self,
        stream: &str,
        matchers: &Matchers,
        value_type: ValueType,
    ) -> Uuid {
        self.store.insert_new_id(stream, matchers, value_type)
    }

    pub fn get_stream_value_type(&self, id: Uuid) -> Option<ValueType> {
        self.store.get_value_type_for_stream_id(id)
    }

    pub fn insert_new_file(&self, id: Uuid, file: &Path, start: Timestamp, end: Timestamp) {
        self.store.insert_new_file(id, file, start, end);
    }

    pub fn get_stream_ids(&self, stream: &str, matchers: &Matchers) -> HashSet<Uuid> {
        let mut id_lists = self.store.get_stream_and_matcher_ids(stream, matchers);
        self.compute_intersection(&mut id_lists)
    }

    fn compute_intersection(&self, id_lists: &mut [HashSet<Uuid>]) -> HashSet<Uuid> {
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

    pub fn get_required_files(
        &self,
        stream_id: Uuid,
        start: Timestamp,
        end: Timestamp,
    ) -> Vec<PathBuf> {
        self.store.get_files_for_stream_id(stream_id, start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::Indexer;
    use crate::utils::test::set_up_dirs;
    use crate::ValueType;
    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use std::collections::HashSet;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn test_intersection() {
        set_up_dirs!(dirs, "db");
        let indexer = Indexer::new(dirs[0].clone());

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();
        let uuid4 = Uuid::new_v4();
        let uuid5 = Uuid::new_v4();

        let hs1 = HashSet::from([uuid1, uuid2, uuid3, uuid4, uuid5]);
        let hs2 = HashSet::from([uuid1, uuid3, uuid5]);
        let hs3 = HashSet::from([uuid1, uuid5]);

        let intersect = indexer.compute_intersection(&mut Vec::from([hs1, hs2, hs3]));

        assert_eq!(intersect, HashSet::from([uuid1, uuid5]));
    }

    #[test]
    fn test_get_required_files_time_range() {
        set_up_dirs!(dirs, "db");

        // seed indexer storage
        let mut indexer = Indexer::new(dirs[0].clone());
        indexer.drop_store();
        indexer.create_store();
        let stream = "https";
        let matchers = Matchers::new(vec![
            Matcher::new(MatchOp::Equal, "app", "dummy"),
            Matcher::new(MatchOp::Equal, "service", "backend"),
        ]);
        let id = indexer.insert_new_id(stream, &matchers, ValueType::UInteger64);

        let file1 = PathBuf::from(format!("{}/{}/file1.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(id, &file1, 1, 3);

        let file2 = PathBuf::from(format!("{}/{}/file2.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(id, &file2, 3, 5);

        let file3 = PathBuf::from(format!("{}/{}/file3.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(id, &file3, 5, 7);

        // query indexer storage
        let mut filenames = indexer.get_required_files(id, 4, 4);
        filenames.sort();
        let mut expected = Vec::from([file2.clone()]);
        assert_eq!(filenames, expected);

        filenames = indexer.get_required_files(id, 2, 6);
        filenames.sort();
        expected = Vec::from([file1, file2, file3]);
        expected.sort();
        assert_eq!(filenames, expected);

        indexer.drop_store();
    }

    #[test]
    fn test_get_required_files_matchers() {
        set_up_dirs!(dirs, "db");

        // seed indexer storage
        let mut indexer = Indexer::new(dirs[0].clone());
        indexer.drop_store();
        indexer.create_store();

        let stream = "https";
        let matchers1 = Matchers::new(vec![
            Matcher::new(MatchOp::Equal, "app", "dummy"),
            Matcher::new(MatchOp::Equal, "service", "backend"),
        ]);
        let id1 = indexer.insert_new_id(stream, &matchers1, ValueType::UInteger64);
        let matchers2 = Matchers::new(vec![
            Matcher::new(MatchOp::Equal, "app", "dummy"),
            Matcher::new(MatchOp::Equal, "service", "frontend"),
        ]);
        let id2 = indexer.insert_new_id(stream, &matchers2, ValueType::UInteger64);

        let file1 = PathBuf::from(format!("{}/{}/file1.ty", dirs[0].to_str().unwrap(), id1));
        indexer.insert_new_file(id1, &file1, 1, 4);

        let file2 = PathBuf::from(format!("{}/{}/file2.ty", dirs[0].to_str().unwrap(), id1));
        indexer.insert_new_file(id1, &file2, 5, 8);

        let file3 = PathBuf::from(format!("{}/{}/file3.ty", dirs[0].to_str().unwrap(), id2));
        indexer.insert_new_file(id2, &file3, 1, 4);

        let file4 = PathBuf::from(format!("{}/{}/file4.ty", dirs[0].to_str().unwrap(), id2));
        indexer.insert_new_file(id2, &file4, 5, 8);

        indexer.drop_store();
    }
}
