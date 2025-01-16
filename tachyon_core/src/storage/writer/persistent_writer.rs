use super::super::MAX_NUM_ENTRIES;
use super::Writer;
use crate::query::indexer::Indexer;
use super::super::file::PartiallyPersistentDataFile;
use crate::{StreamId, Timestamp, Value, ValueType, Version, FILE_EXTENSION};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use uuid::Uuid;

pub struct PersistentWriter {
    open_data_files: HashMap<Uuid, PartiallyPersistentDataFile>, // Stream ID to in-mem file
    root: PathBuf,
    indexer: Rc<RefCell<Indexer>>,
    version: Version,
}

impl PersistentWriter {
    fn derive_file_path(root: impl AsRef<Path>, stream_id: Uuid, ts: Timestamp) -> PathBuf {
        root.as_ref().join(format!(
            "{}/{}.{}",
            stream_id,
            ts,
            FILE_EXTENSION
        ))
    }
}

impl Writer for PersistentWriter {
    fn new(root: impl AsRef<Path>, indexer: Rc<RefCell<Indexer>>, version: Version) -> Self {
        PersistentWriter {
            open_data_files: HashMap::new(),
            root: root.as_ref().to_path_buf(),
            indexer,
            version,
        }
    }

    fn write(&mut self, stream_id: Uuid, ts: Timestamp, v: Value, value_type: ValueType) {
        if let Some(file) = self.open_data_files.get_mut(&stream_id) {
            // Use the existing file if available
            file.write(ts, v).unwrap();
            if file.num_entries() >= MAX_NUM_ENTRIES {
                // persist any un
                file.flush().unwrap();
                self.open_data_files.remove_entry(&stream_id);
            }

        } else {
            // Derive the file path and create a new file if it doesn't exist
            let file_path = PersistentWriter::derive_file_path(&self.root, stream_id, ts);
        
            let new_file = PartiallyPersistentDataFile::new(
                self.version,
                StreamId(stream_id.as_u128()),
                value_type,
                self.indexer.clone(),
                file_path.clone(),
            )
            .lazy_init(ts, v);
                
            // Insert the new file into open_data_files
            self.open_data_files.insert(stream_id, new_file);
        }
    }

    fn flush_all(&mut self) {
        for (_, file) in self.open_data_files.iter_mut() {
            file.flush().unwrap();
        }
        self.open_data_files.clear();
    }

    fn create_stream(&self, stream_id: Uuid) {
        let stream = self.root.join(stream_id.to_string());
        if !stream.exists() {
            fs::create_dir(stream).unwrap();
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::*;
    use std::fs;
    use super::super::super::file::TimeDataFile;

    // Gets all files from directory sorted from smallest to highest file name suffix
    fn get_files(dir: &Path) -> Vec<TimeDataFile> {
        let read_dir = fs::read_dir(dir).unwrap();

        let mut paths: Vec<PathBuf> = Vec::new();
        let mut files: Vec<TimeDataFile> = Vec::new();

        for path in read_dir {
            paths.push(path.unwrap().path());
        }

        fn extract_end(path_buf: &Path) -> u32 {
            let path = path_buf
                .to_path_buf()
                .into_os_string()
                .into_string()
                .unwrap();

            let suffix_opt = path
                .rsplit('/')
                .next()
                .and_then(|num_str| num_str.split('.').next().unwrap().parse::<u32>().ok());

            suffix_opt.expect("Expected file suffix to be u32")
        }

        paths.sort_by(|a, b| {
            let num_a = extract_end(a);
            let num_b = extract_end(b);

            num_a.cmp(&num_b)
        });

        for path in paths {
            files.push(TimeDataFile::read_data_file(path));
        }

        files
    }

    #[test]
    fn test_write_single_complete_file() {
        set_up_dirs!(dirs, "db");
        let stream_id = Uuid::new_v4();

        let indexer = Rc::new(RefCell::new(Indexer::new(dirs[0].clone())));
        indexer.borrow_mut().create_store();

        let mut writer = PersistentWriter::new(dirs[0].clone(), indexer, Version(0));
        let mut timestamps = Vec::<Timestamp>::new();
        let mut values = Vec::<Value>::new();

        writer.create_stream(stream_id);

        for i in 0..MAX_NUM_ENTRIES as u64 {
            let ts = i as Timestamp;
            let v = (i * 1000).into();
            writer.write(stream_id, ts, v, ValueType::UInteger64);
            timestamps.push(ts);
            values.push(v);
        }

        let files = get_files(&dirs[0].join(stream_id.to_string()));

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].timestamps, timestamps);
        assert_eq!(files[0].values.len(), values.len());
        #[allow(clippy::needless_range_loop)]
        for i in 0..values.len() {
            assert!(files[0].values[i].eq_same(ValueType::UInteger64, &values[i]));
        }
        for i in 0..values.len() {
            assert!(files[0].timestamps[i] == timestamps[i]);
        }
    }

    #[test]
    fn test_write_multiple_streams_single_complete_files() {
        set_up_dirs!(dirs, "db");
        let stream_ids = [Uuid::new_v4(), Uuid::new_v4()];

        let indexer = Rc::new(RefCell::new(Indexer::new(dirs[0].clone())));
        indexer.borrow_mut().create_store();
        let mut writer = PersistentWriter::new(dirs[0].clone(), indexer, Version(0));

        let mut timestamps = [Vec::<Timestamp>::new(), Vec::<Timestamp>::new()];
        let mut values = [Vec::<Value>::new(), Vec::<Value>::new()];

        for stream_id in stream_ids {
            writer.create_stream(stream_id);
        }

        for i in 0..MAX_NUM_ENTRIES as u64 {
            for (j, stream_id) in stream_ids.iter().enumerate() {
                let ts = i as Timestamp;
                let v = (i * 1000).into();
                writer.write(*stream_id, ts, v, ValueType::UInteger64);
                timestamps[j].push(ts);
                values[j].push(v);
            }
        }

        for stream_id in stream_ids {
            let files = get_files(&dirs[0].join(stream_id.to_string()));
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].timestamps, timestamps[0]);
            assert_eq!(files[0].values.len(), values[0].len());
            for i in 0..values[0].len() {
                assert!(files[0].values[i].eq_same(ValueType::UInteger64, &values[0][i]));
            }
            for i in 0..values.len() {
                assert!(files[0].timestamps[i] == timestamps[0][i]);
            }
        }
    }

    #[test]
    fn test_batch_write_single_batch_in_three_files() {
        let stream_id = Uuid::new_v4();
        set_up_dirs!(dirs, "db");

        let n = (1.5 * MAX_NUM_ENTRIES as f32).round() as usize;
        let mut base: usize = 0;

        let indexer = Rc::new(RefCell::new(Indexer::new(dirs[0].clone())));
        indexer.borrow_mut().create_store();
        let mut writer = PersistentWriter::new(dirs[0].clone(), indexer, Version(0));
        let mut timestamps_per_file = [
            Vec::<Timestamp>::new(),
            Vec::<Timestamp>::new(),
            Vec::<Timestamp>::new(),
        ];
        let mut values_per_file = [
            Vec::<Value>::new(),
            Vec::<Value>::new(),
            Vec::<Value>::new(),
        ];
        let mut count = 0;

        fn create_and_write_batch(
            n: usize,
            base: usize,
            timestamps_per_file: &mut [Vec<Timestamp>],
            values_per_file: &mut [Vec<Value>],
            count: &mut usize,
            writer: &mut PersistentWriter,
            stream_id: Uuid,
        ) {
            for i in 0..n {
                let timestamp = (base + i) as Timestamp;
                let value = ((i * 1000) as u64).into();
                timestamps_per_file[*count / MAX_NUM_ENTRIES].push(timestamp);
                values_per_file[*count / MAX_NUM_ENTRIES].push(value);
                *count += 1;
                writer.write(stream_id, timestamp, value, ValueType::UInteger64);
            }
        }

        writer.create_stream(stream_id);

        for _ in 0..2 {
            create_and_write_batch(
                n,
                base,
                &mut timestamps_per_file,
                &mut values_per_file,
                &mut count,
                &mut writer,
                stream_id,
            );
            base += n;
        }

        let files = get_files(&dirs[0].join(stream_id.to_string()));
        assert_eq!(files.len(), 3);

        for i in 0..3 {
            assert_eq!(files[i].timestamps, timestamps_per_file[i]);
            assert_eq!(files[i].values.len(), values_per_file[i].len());
            for j in 0..values_per_file[i].len() {
                assert!(files[i].values[j].eq_same(ValueType::UInteger64, &values_per_file[i][j]));
            }
        }
    }
}
