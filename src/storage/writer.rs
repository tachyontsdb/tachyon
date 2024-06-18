use std::{collections::HashMap, mem::size_of, path::Path, path::PathBuf};

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
            file.write(Writer::derive_file_path(&(self.root), stream_id, file));
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
                file.write(Writer::derive_file_path(&(self.root), stream_id, file));
                self.open_data_files.remove_entry(&stream_id);
            }
        }
    }

    fn derive_file_path(root: &Path, stream_id: u64, file: &TimeDataFile) -> PathBuf {
        root.join(format!("{}/{}", stream_id, file.get_file_name()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::*;
    use std::fs;

    const MAX_ENTRIES: usize = MAX_FILE_SIZE / (2 * size_of::<u64>());

    #[test]
    fn test_write_single_complete_file() {
        set_up_dirs!(dirs, "0",);

        let mut writer = Writer::new("./tmp/test_write_single_complete_file$".into());
        let mut timestamps = Vec::<Timestamp>::new();
        let mut values = Vec::<Value>::new();

        for i in 0..MAX_ENTRIES {
            writer.write(0, i as Timestamp, (i * 1000) as Value);
        }
    }

    #[test]
    fn test_write_multiple_complete_files() {
        let stream_ids = [0, 1];
        set_up_dirs!(dirs, "0", "1",);

        let mut writer = Writer::new("./tmp/test_write_multiple_complete_files$".into());
        let mut timestamps = Vec::<Timestamp>::new();
        let mut values = Vec::<Value>::new();

        for i in 0..MAX_ENTRIES {
            for stream_id in stream_ids {
                writer.write(stream_id, i as Timestamp, (i * 1000) as Value);
            }
        }
    }

    #[test]
    fn test_batch_write_single_batch_in_two_files() {
        let stream_id = 0;
        set_up_dirs!(dirs, "0",);

        let n = (1.5 * MAX_ENTRIES as f32).round() as usize;
        let mut writer = Writer::new("./tmp/test_batch_write_single_batch_in_two_files$".into());
        let mut entries: Vec<(u64, u64)> = Vec::<(Timestamp, Value)>::with_capacity(n);

        for i in 0..n {
            entries.push((i as Timestamp, (i * 1000) as Value));
        }

        writer.batch_write(stream_id, &entries);

        let old_n = n;
        let n = (0.5 * MAX_ENTRIES as f32).round() as usize;
        let mut entries: Vec<(u64, u64)> = Vec::<(Timestamp, Value)>::with_capacity(n);
        for i in 0..n {
            entries.push(((old_n + i) as Timestamp, ((old_n + i) * 1000) as Value));
        }

        writer.batch_write(stream_id, &entries);
    }
}
