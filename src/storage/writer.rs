use std::{collections::HashMap, mem::size_of, path::PathBuf};

use crate::common::{Timestamp, Value};

use super::file::{TimeDataFile, MAX_FILE_SIZE};

struct Writer {
    open_data_files: HashMap<u64, TimeDataFile>, // stream id to in-mem file
    root: PathBuf,
}

impl Writer {
    pub fn new(root: PathBuf) -> Self {
        Writer {
            open_data_files: HashMap::new(),
            root,
        }
    }

    pub fn write(&mut self, stream_id: u64, ts: Timestamp, v: Value) {
        let file = self.open_data_files.entry(stream_id).or_default();

        file.write_data_to_file_in_mem(ts, v);
        if file.size_of_entries() >= MAX_FILE_SIZE {
            file.write(self.root.join(format!("{}", stream_id)));
            self.open_data_files.remove_entry(&stream_id);
        }
    }

    pub fn batch_write(&mut self, stream_id: u64, batch: &[(Timestamp, Value)]) {
        let mut bytes_written: usize = 0;
        let n_bytes = std::mem::size_of_val(batch);
        let mut i = 0;

        while bytes_written != n_bytes {
            let file = self.open_data_files.entry(stream_id).or_default();

            bytes_written += file.write_batch_data_to_file_in_mem(&batch[i..]);
            i = bytes_written / size_of::<(Timestamp, Value)>();

            if file.size_of_entries() >= MAX_FILE_SIZE {
                file.write(self.root.join(format!("{}", stream_id)));
                self.open_data_files.remove_entry(&stream_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const MAX_ENTRIES: usize = MAX_FILE_SIZE / (2 * size_of::<u64>());

    fn init(stream_ids: &[u32]) {
        fs::create_dir("./tmp/db");
        for stream_id in stream_ids {
            fs::create_dir(format!("./tmp/db/{}", stream_id));
        }
    }

    fn clean(stream_ids: &[u32]) {
        for stream_id in stream_ids {
            fs::remove_dir_all(format!("./tmp/db/{}", stream_id));
        }
        fs::remove_dir("./tmp/db");
    }

    #[test]
    fn test_write_single_complete_file() {
        let stream_ids = [0];
        init(&stream_ids);

        let mut writer = Writer::new("./tmp/db".into());
        let mut timestamps = Vec::<Timestamp>::new();
        let mut values = Vec::<Value>::new();

        for i in 0..MAX_ENTRIES {
            writer.write(0, i as Timestamp, (i * 1000) as Value);
        }

        clean(&stream_ids)
    }

    #[test]
    fn test_write_multiple_complete_files() {
        let stream_ids = [0, 1];
        init(&stream_ids);

        let mut writer = Writer::new("./tmp/db".into());
        let mut timestamps = Vec::<Timestamp>::new();
        let mut values = Vec::<Value>::new();

        for i in 0..MAX_ENTRIES {
            for stream_id in stream_ids {
                writer.write(stream_id.into(), i as Timestamp, (i * 1000) as Value);
            }
        }

        clean(&stream_ids);
    }

    #[test]
    fn test_batch_write_single_batch_in_two_files() {
        let stream_ids = [0];
        init(&stream_ids);

        let n = (1.5 * MAX_ENTRIES as f32).round() as usize;
        let mut writer = Writer::new("./tmp/db".into());
        let mut entries: Vec<(u64, u64)> = Vec::<(Timestamp, Value)>::with_capacity(n);

        for i in 0..n {
            entries.push((i as Timestamp, (i * 1000) as Value));
        }

        for stream_id in stream_ids {
            writer.batch_write(stream_id.into(), &entries);
        }

        let old_n = n;
        let n = (0.5 * MAX_ENTRIES as f32).round() as usize;
        let mut entries: Vec<(u64, u64)> = Vec::<(Timestamp, Value)>::with_capacity(n);
        for i in 0..n {
            entries.push(((old_n + i) as Timestamp, ((old_n + i) * 1000) as Value));
        }

        for stream_id in stream_ids {
            writer.batch_write(stream_id.into(), &entries);
        }

        clean(&stream_ids);
    }
}
