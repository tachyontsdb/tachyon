use core::num;
use std::{
    collections::HashMap,
    fs::File,
    io::{Empty, Read, Seek},
    iter::Map,
    mem::MaybeUninit,
    path::PathBuf,
};

const FILE_SIZE: usize = 1_000_000;
const PAGE_SIZE: usize = 4_000;

struct PageInfo {
    file_id: usize,
    page_id: usize,
    data: [u8; PAGE_SIZE],
}

enum Frame {
    Empty,
    Page(PageInfo),
}

pub struct PageCache {
    frames: Vec<Frame>,

    // fileId, pageid -> frameId
    mapping: HashMap<(usize, usize), usize>,
    open_files: HashMap<usize, File>,

    file_path_to_id: HashMap<String, usize>,
    file_id_to_path: HashMap<usize, String>,
    cur_file_id: usize,

    root_free: usize,
}

impl PageCache {
    pub fn new(num_frames: usize) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        for i in 0..num_frames {
            frames.push(Frame::Empty);
        }

        PageCache {
            frames,
            mapping: HashMap::with_capacity(num_frames),
            open_files: HashMap::with_capacity(num_frames * PAGE_SIZE / FILE_SIZE),
            file_path_to_id: HashMap::with_capacity(num_frames * PAGE_SIZE / FILE_SIZE),
            file_id_to_path: HashMap::with_capacity(num_frames * PAGE_SIZE / FILE_SIZE),
            cur_file_id: 0,
            root_free: 0,
        }
    }

    pub fn register_or_get_file_id(&mut self, path: &String) -> usize {
        if let Some(id) = self.file_path_to_id.get(path) {
            return *id;
        }
        self.file_path_to_id.insert(path.clone(), self.cur_file_id);
        self.file_id_to_path.insert(self.cur_file_id, path.clone());
        self.cur_file_id += 1;
        self.cur_file_id - 1
    }

    pub fn read(&mut self, file_id: usize, mut offset: usize, buffer: &mut [u8]) -> usize {
        let last_offset = offset + buffer.len();
        let first_page_id = offset / PAGE_SIZE;
        let last_page_id = last_offset / PAGE_SIZE;
        let mut bytes_copied = 0;

        for page_id in first_page_id..last_page_id + 1 {
            let frame_id;
            // 1st - check that page_id is loaded in memory
            if let Some(frame) = self.mapping.get(&(file_id, page_id)) {
                frame_id = *frame;
            } else {
                // check that file is open
                if let std::collections::hash_map::Entry::Vacant(e) = self.open_files.entry(file_id) {
                    let path = self.file_id_to_path.get(&file_id).unwrap();
                    e.insert(File::open(path).unwrap());
                }

                // find next available frame
                // TODO: Change eviction scheme
                frame_id = self.root_free;
                self.root_free = (self.root_free + 1) % self.frames.len();

                // if there was a page there, remove it from mapping
                if let Frame::Page(info) = &mut self.frames[frame_id] {
                    // evict
                    self.mapping.remove(&(info.file_id, info.page_id));
                }

                let mut new_page_info = PageInfo {
                    file_id,
                    page_id,
                    data: [0; PAGE_SIZE],
                };
                self.open_files
                    .get_mut(&file_id)
                    .unwrap()
                    .seek(std::io::SeekFrom::Start((PAGE_SIZE * page_id) as u64));

                let bytes_read = self
                    .open_files
                    .get_mut(&file_id)
                    .unwrap()
                    .read(&mut new_page_info.data)
                    .unwrap();

                self.mapping.insert((file_id, page_id), frame_id);
                self.frames[frame_id] = Frame::Page(new_page_info);
            }

            // page is now guaranteed to be loaded
            if let Frame::Page(PageInfo {
                file_id,
                page_id,
                data,
            }) = &mut self.frames[frame_id]
            {
                let num_bytes =
                    (PAGE_SIZE - (offset % PAGE_SIZE)).min((buffer.len() - bytes_copied));
                let data_to_copy = &data[(offset % PAGE_SIZE)..(offset % PAGE_SIZE) + num_bytes];
                offset += num_bytes;
                buffer[bytes_copied..bytes_copied + num_bytes].clone_from_slice(data_to_copy);
                bytes_copied += num_bytes;
            } else {
                assert!(false);
            }
        }

        bytes_copied
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write, path::PathBuf, str::FromStr};

    use crate::{
        common::{Timestamp, Value},
        storage::file::TimeDataFile,
    };

    use super::PageCache;

    #[test]
    fn test_read_whole_file() {
        let mut page_cache = PageCache::new(10);
        let mut model = TimeDataFile::new();
        for i in 0..100000u64 {
            model.write_data_to_file_in_mem(i, i + 10);
        }
        let file_size = model.write("./tmp/page_cache_read.ty".into());
        // start the test
        let file_id = page_cache.register_or_get_file_id(&"./tmp/page_cache_read.ty".to_owned());
        assert_eq!(file_id, 0);

        let mut buffer = vec![0; file_size];
        page_cache.read(file_id, 0, &mut buffer);

        let mut new_file = File::create("./tmp/page_cache_test.ty").unwrap();
        new_file.write_all(&buffer);

        let data_file =
            TimeDataFile::read_data_file(PathBuf::from_str("./tmp/page_cache_test.ty").unwrap());

        assert_eq!(data_file.timestamps.len(), 100000);
        for i in 0..data_file.timestamps.len() {
            assert_eq!(data_file.timestamps[i], i as Timestamp);
            assert_eq!(data_file.values[i], (i + 10) as Value);
        }
        std::fs::remove_file("./tmp/page_cache_read.ty");
        std::fs::remove_file("./tmp/page_cache_test.ty");
    }
}
