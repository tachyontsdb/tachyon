use crate::{StreamSummaryType, Timestamp, ValueType};
use promql_parser::label::Matchers;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use uuid::Uuid;

trait IndexerStore {
    fn create_store(&mut self);
    fn drop_store(&mut self);

    fn get_all_streams(&self) -> Vec<StreamSummaryType>;
    fn get_value_type_for_stream_id(&self, stream_id: Uuid) -> Option<ValueType>;

    fn insert_new_id(&mut self, stream: &str, matchers: &Matchers, value_type: ValueType) -> Uuid;
    fn insert_new_file(&mut self, id: Uuid, file: &Path, start: Timestamp, end: Option<Timestamp>);
    fn insert_or_replace_file(&mut self, id: Uuid, file: &Path, start: Timestamp, end: Timestamp);

    fn get_stream_and_matcher_ids(&self, stream: &str, matchers: &Matchers) -> Vec<HashSet<Uuid>>;
    fn get_files_for_stream_id(
        &self,
        stream_id: Uuid,
        start: Timestamp,
        end: Timestamp,
    ) -> Vec<PathBuf>;
}

mod sqlite {
    use super::*;
    use rusqlite::Connection;
    use std::collections::HashMap;

    pub struct SQLiteIndexerStore {
        conn: Connection,
    }

    impl SQLiteIndexerStore {
        // SQLite Store Constants
        const SQLITE_DB_NAME: &str = "indexer.sqlite";

        const SQLITE_STREAM_TO_IDS_TABLE: &str = "stream_to_ids";
        const SQLITE_ID_TO_FILENAME_TABLE: &str = "id_to_file";
        const SQLITE_ID_TO_VALUE_TYPE_TABLE: &str = "id_to_value_type";

        const SQLITE_STREAM_NAME_COLUMN: &str = "__name";

        pub fn new(root_dir: impl AsRef<Path>) -> Self {
            Self {
                conn: Connection::open(root_dir.as_ref().join(Self::SQLITE_DB_NAME)).unwrap(),
            }
        }
    }

    impl SQLiteIndexerStore {
        fn get_ids_or_empty(&self, name: &str, value: &str) -> HashSet<Uuid> {
            self.conn
                .query_row(
                    &format!(
                        "SELECT ids FROM {} WHERE name = ? AND value = ?",
                        Self::SQLITE_STREAM_TO_IDS_TABLE
                    ),
                    [name, value],
                    |row| row.get::<usize, String>(0),
                )
                .map_or_else(
                    |_| HashSet::new(),
                    |stream_ids_str| serde_json::from_str(&stream_ids_str).unwrap(),
                )
        }

        fn get_stream_and_matchers_for_stream_id(&self, stream_id: Uuid) -> Vec<(String, String)> {
            let mut stmt = self
                .conn
                .prepare_cached(&format!(
                    "SELECT name, value, ids FROM {}",
                    Self::SQLITE_STREAM_TO_IDS_TABLE
                ))
                .unwrap();

            let rows = stmt
                .query_map((), |row| {
                    Ok((
                        row.get::<usize, String>(0).unwrap(),
                        row.get::<usize, String>(1).unwrap(),
                        row.get::<usize, String>(2).unwrap(),
                    ))
                })
                .unwrap();

            let mut result = Vec::<(String, String)>::new();

            for item in rows {
                let (name, value, stream_ids_str) = item.unwrap();

                let stream_ids: HashSet<Uuid> = serde_json::from_str(&stream_ids_str).unwrap();
                if stream_ids.contains(&stream_id) {
                    result.push((name, value));
                }
            }

            result
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
                        Self::SQLITE_STREAM_TO_IDS_TABLE
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
                            filename TEXT,
                            start INTEGER,
                            end INTEGER,
                            PRIMARY KEY (id, filename)
                        )
                    ",
                        Self::SQLITE_ID_TO_FILENAME_TABLE
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
                        Self::SQLITE_ID_TO_VALUE_TYPE_TABLE
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
                    &format!("DROP TABLE IF EXISTS {}", Self::SQLITE_STREAM_TO_IDS_TABLE),
                    (),
                )
                .unwrap();

            transaction
                .execute(
                    &format!("DROP TABLE IF EXISTS {}", Self::SQLITE_ID_TO_FILENAME_TABLE),
                    (),
                )
                .unwrap();

            transaction
                .execute(
                    &format!(
                        "DROP TABLE IF EXISTS {}",
                        Self::SQLITE_ID_TO_VALUE_TYPE_TABLE
                    ),
                    (),
                )
                .unwrap();

            transaction.commit().unwrap();
        }

        fn insert_new_id(
            &mut self,
            stream: &str,
            matchers: &Matchers,
            value_type: ValueType,
        ) -> Uuid {
            let new_id = Uuid::new_v4();

            // Get old ids and add new one
            let mut stream_ids = self.get_ids_or_empty(Self::SQLITE_STREAM_NAME_COLUMN, stream);
            stream_ids.insert(new_id);

            let mut matcher_ids_map = HashMap::<String, HashSet<Uuid>>::default();
            for matcher in &matchers.matchers {
                let mut matcher_ids = self.get_ids_or_empty(&matcher.name, &matcher.value);
                matcher_ids.insert(new_id);

                matcher_ids_map.insert(matcher.name.clone(), matcher_ids);
            }

            // Commit changes to db
            let transaction = self.conn.transaction().unwrap();

            let mut stmt = transaction
                .prepare_cached(&format!(
                    "INSERT OR REPLACE INTO {} (name, value, ids) VALUES (?, ?, ?)",
                    Self::SQLITE_STREAM_TO_IDS_TABLE
                ))
                .unwrap();

            let stream_id_str = serde_json::to_string(&stream_ids).unwrap();
            stmt.execute([Self::SQLITE_STREAM_NAME_COLUMN, stream, &stream_id_str])
                .unwrap();

            for matcher in &matchers.matchers {
                let matcher_ids_str =
                    serde_json::to_string(matcher_ids_map.get(&matcher.name).unwrap()).unwrap();
                stmt.execute([&matcher.name, &matcher.value, &matcher_ids_str])
                    .unwrap();
            }

            transaction
                .execute(
                    &format!(
                        "INSERT INTO {} (id, value_type) VALUES (?, ?)",
                        Self::SQLITE_ID_TO_VALUE_TYPE_TABLE
                    ),
                    (serde_json::to_string(&new_id).unwrap(), value_type as u8),
                )
                .unwrap();

            drop(stmt);
            transaction.commit().unwrap();

            new_id
        }

        fn insert_new_file(&mut self, id: Uuid, file: &Path, start: Timestamp, end: Option<Timestamp>) {
            self.conn
                .execute(
                    &format!(
                        "INSERT INTO {} (id, filename, start, end) VALUES (?, ?, ?, ?)",
                        Self::SQLITE_ID_TO_FILENAME_TABLE
                    ),
                    (id, file.to_str(), start, end),
                )
                .unwrap();
        }

        fn insert_or_replace_file(&mut self, id: Uuid, file: &Path, start: Timestamp, end: Timestamp) {
            self.conn
                .execute(
                    &format!(
                        "INSERT OR REPLACE INTO {} (id, filename, start, end) VALUES (?, ?, ?, ?)",
                        Self::SQLITE_ID_TO_FILENAME_TABLE
                    ),
                    (id, file.to_str(), start, end),
                )
                .unwrap();
        }

        fn get_stream_and_matcher_ids(
            &self,
            stream: &str,
            matchers: &Matchers,
        ) -> Vec<HashSet<Uuid>> {
            let mut ids = Vec::<HashSet<Uuid>>::new();

            ids.push(self.get_ids_or_empty(Self::SQLITE_STREAM_NAME_COLUMN, stream));
            for matcher in &matchers.matchers {
                ids.push(self.get_ids_or_empty(&matcher.name, &matcher.value));
            }

            ids
        }

        fn get_files_for_stream_id(
            &self,
            stream_id: Uuid,
            start: Timestamp,
            end: Timestamp,
        ) -> Vec<PathBuf> {
            let mut stmt = self
                .conn
                .prepare_cached(&format!(
                    "SELECT filename FROM {} WHERE id = ? AND (? <= end OR end IS NULL) AND ? >= start ORDER BY start ASC",
                    Self::SQLITE_ID_TO_FILENAME_TABLE
                ))
                .unwrap();

            stmt.query_map((stream_id, start, end), |row| row.get::<usize, String>(0))
                .unwrap()
                .map(|item| item.unwrap().into())
                .collect()
        }

        fn get_value_type_for_stream_id(&self, stream_id: Uuid) -> Option<ValueType> {
            self.conn
                .query_row(
                    &format!(
                        "SELECT value_type FROM {} WHERE id = ?",
                        Self::SQLITE_ID_TO_VALUE_TYPE_TABLE
                    ),
                    [serde_json::to_string(&stream_id).unwrap()],
                    |row| row.get::<usize, u8>(0),
                )
                .map_or_else(|_| None, |value_type| Some(value_type.try_into().unwrap()))
        }

        fn get_all_streams(&self) -> Vec<StreamSummaryType> {
            let mut stmt = self
                .conn
                .prepare_cached(&format!(
                    "SELECT id, value_type FROM {}",
                    Self::SQLITE_ID_TO_VALUE_TYPE_TABLE
                ))
                .unwrap();

            stmt.query_map((), |row| {
                Ok((
                    row.get::<usize, String>(0).unwrap(),
                    row.get::<usize, u8>(1).unwrap(),
                ))
            })
            .unwrap()
            .map(|item| {
                let (stream_id_str, value_type_u8) = item.unwrap();
                let stream_id = serde_json::from_str(&stream_id_str).unwrap();
                (
                    stream_id,
                    self.get_stream_and_matchers_for_stream_id(stream_id),
                    value_type_u8.try_into().unwrap(),
                )
            })
            .collect()
        }
    }
}

pub struct Indexer {
    store: Box<dyn IndexerStore>,
}

impl Indexer {
    pub fn new(root_dir: impl AsRef<Path>) -> Self {
        Self {
            store: Box::new(sqlite::SQLiteIndexerStore::new(root_dir)),
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

    pub fn insert_new_file(&mut self, id: Uuid, file: &Path, start: Timestamp, end: Option<Timestamp>) {
        self.store.insert_new_file(id, file, start, end);
    }

    pub fn insert_or_replace_file(&mut self, id: Uuid, file: &Path, start: Timestamp, end: Timestamp) {
        self.store.insert_or_replace_file(id, file, start, end);
    }

    pub fn get_all_streams(&self) -> Vec<StreamSummaryType> {
        self.store.get_all_streams()
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
        indexer.insert_new_file(id, &file1, 1, Some(3));

        let file2 = PathBuf::from(format!("{}/{}/file2.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(id, &file2, 3, Some(5));

        let file3 = PathBuf::from(format!("{}/{}/file3.ty", dirs[0].to_str().unwrap(), id));
        indexer.insert_new_file(id, &file3, 5, Some(7));

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
        indexer.insert_new_file(id1, &file1, 1, Some(4));

        let file2 = PathBuf::from(format!("{}/{}/file2.ty", dirs[0].to_str().unwrap(), id1));
        indexer.insert_new_file(id1, &file2, 5, Some(8));

        let file3 = PathBuf::from(format!("{}/{}/file3.ty", dirs[0].to_str().unwrap(), id2));
        indexer.insert_new_file(id2, &file3, 1, Some(4));

        let file4 = PathBuf::from(format!("{}/{}/file4.ty", dirs[0].to_str().unwrap(), id2));
        indexer.insert_new_file(id2, &file4, 5, Some(8));

        indexer.drop_store();
    }

    #[test]
    fn test_get_value_type_for_stream() {
        set_up_dirs!(dirs, "db");

        let mut indexer = Indexer::new(dirs[0].clone());
        indexer.drop_store();
        indexer.create_store();

        let (stream1, matchers1, stream_value_type_1) = (
            "str1",
            Matchers::new(vec![Matcher::new(MatchOp::Equal, "a", "b")]),
            ValueType::UInteger64,
        );
        let s1id = indexer.insert_new_id(stream1, &matchers1, stream_value_type_1);

        let (stream2, matchers2, stream_value_type_2) = (
            "str2",
            Matchers::new(vec![Matcher::new(MatchOp::Equal, "c", "d")]),
            ValueType::Integer64,
        );
        let s2id = indexer.insert_new_id(stream2, &matchers2, stream_value_type_2);

        let (stream3, matchers3, stream_value_type_3) = (
            "str3",
            Matchers::new(vec![Matcher::new(MatchOp::Equal, "e", "f")]),
            ValueType::Float64,
        );
        let s3id = indexer.insert_new_id(stream3, &matchers3, stream_value_type_3);

        assert_eq!(
            indexer.get_stream_value_type(s1id),
            Some(ValueType::UInteger64)
        );
        assert_eq!(
            indexer.get_stream_value_type(s2id),
            Some(ValueType::Integer64)
        );
        assert_eq!(
            indexer.get_stream_value_type(s3id),
            Some(ValueType::Float64)
        );
    }

    #[test]
    fn test_get_all_streams() {
        set_up_dirs!(dirs, "db");

        let mut indexer = Indexer::new(dirs[0].clone());
        indexer.drop_store();
        indexer.create_store();

        let (stream1, matchers1, stream_value_type_1) = (
            "str1",
            Matchers::new(vec![Matcher::new(MatchOp::Equal, "a", "b")]),
            ValueType::UInteger64,
        );
        let s1id = indexer.insert_new_id(stream1, &matchers1, stream_value_type_1);

        let (stream2, matchers2, stream_value_type_2) = (
            "str2",
            Matchers::new(vec![Matcher::new(MatchOp::Equal, "c", "d")]),
            ValueType::Integer64,
        );
        let s2id = indexer.insert_new_id(stream2, &matchers2, stream_value_type_2);

        let (stream3, matchers3, stream_value_type_3) = (
            "str3",
            Matchers::new(vec![Matcher::new(MatchOp::Equal, "e", "f")]),
            ValueType::Float64,
        );
        let s3id = indexer.insert_new_id(stream3, &matchers3, stream_value_type_3);

        let all_streams = indexer.get_all_streams();
        assert_eq!(all_streams.len(), 3);

        assert_eq!(all_streams[0].0, s1id);
        assert_eq!(all_streams[1].0, s2id);
        assert_eq!(all_streams[2].0, s3id);

        assert!(all_streams[0].1.contains(&("__name".into(), "str1".into())));
        assert!(all_streams[1].1.contains(&("__name".into(), "str2".into())));
        assert!(all_streams[2].1.contains(&("__name".into(), "str3".into())));

        assert!(all_streams[0].1.contains(&("a".into(), "b".into())));
        assert!(all_streams[1].1.contains(&("c".into(), "d".into())));
        assert!(all_streams[2].1.contains(&("e".into(), "f".into())));

        assert_eq!(all_streams[0].2, ValueType::UInteger64);
        assert_eq!(all_streams[1].2, ValueType::Integer64);
        assert_eq!(all_streams[2].2, ValueType::Float64);
    }
}
