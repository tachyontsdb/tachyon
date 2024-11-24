use super::hash_map::IDLookup;
use core::num;
use rustc_hash::FxHashMap;
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    hash::{BuildHasherDefault, Hash, Hasher},
    io::{Empty, Read, Seek},
    iter::Map,
    mem::MaybeUninit,
    os::unix::fs::FileExt,
    path::PathBuf,
    rc::Rc,
};

pub type FileId = u32;
type PageId = u32;
type FrameId = usize;

const PAGE_SIZE: usize = 32_768;

struct PageInfo {
    file_id: FileId,
    page_id: PageId,
    data: [u8; PAGE_SIZE],
}

#[allow(clippy::large_enum_variant)]
enum Frame {
    Empty,
    Page(PageInfo),
}

#[derive(Default, Clone, Copy)]
pub struct FastNoHash(u64);
impl Hasher for FastNoHash {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        panic!("No bytes please");
    }

    fn write_u32(&mut self, i: u32) {
        self.0 = i as u64;
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

pub struct PageCache {
    frames: Vec<Frame>,

    mapping: IDLookup<FrameId>,
    open_files: HashMap<FileId, File, BuildHasherDefault<FastNoHash>>,

    file_path_to_id: FxHashMap<PathBuf, FileId>,
    file_id_to_path: HashMap<FileId, PathBuf, BuildHasherDefault<FastNoHash>>,
    cur_file_id: FileId,

    root_free: usize,
}

pub struct SeqPageRead {
    file_id: FileId,
    cur_page_id: PageId,
    frame_id: FrameId,
    pub page_cache: Rc<RefCell<PageCache>>,
    offset: usize,
}

impl SeqPageRead {
    pub fn read(&mut self, buffer: &mut [u8]) -> usize {
        let mut page_cache = self.page_cache.borrow_mut();
        let mut bytes_copied = 0;

        while (bytes_copied < buffer.len()) {
            // make sure correct page is in the frame (not evicted)
            if let Frame::Page(PageInfo {
                file_id, page_id, ..
            }) = &page_cache.frames[self.frame_id]
            {
                if self.file_id != *file_id || self.cur_page_id != *page_id {
                    self.frame_id = page_cache.load_page(self.file_id, self.cur_page_id);
                }
            } else {
                self.frame_id = page_cache.load_page(self.file_id, self.cur_page_id);
            }

            if let Frame::Page(PageInfo { data, .. }) = &page_cache.frames[self.frame_id] {
                let num_bytes =
                    (PAGE_SIZE - (self.offset % PAGE_SIZE)).min(buffer.len() - bytes_copied);
                let data_to_copy =
                    &data[(self.offset % PAGE_SIZE)..(self.offset % PAGE_SIZE) + num_bytes];
                self.offset += num_bytes;
                buffer[bytes_copied..bytes_copied + num_bytes].copy_from_slice(data_to_copy);
                bytes_copied += num_bytes;

                if self.offset % PAGE_SIZE == 0 {
                    self.cur_page_id += 1;
                }
            }
        }

        bytes_copied
    }
}

impl Read for SeqPageRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(self.read(buf))
    }
}

impl PageCache {
    pub fn new(num_frames: usize) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        for i in 0..num_frames {
            frames.push(Frame::Empty);
        }

        PageCache {
            frames,
            mapping: IDLookup::new_with_size(2 * num_frames),
            open_files: HashMap::with_capacity_and_hasher(
                2,
                BuildHasherDefault::<FastNoHash>::default(),
            ),
            file_path_to_id: FxHashMap::default(),
            file_id_to_path: HashMap::with_capacity_and_hasher(
                2,
                BuildHasherDefault::<FastNoHash>::default(),
            ),
            cur_file_id: 0,
            root_free: 0,
        }
    }

    pub fn register_or_get_file_id(&mut self, path: &PathBuf) -> FileId {
        if let Some(id) = self.file_path_to_id.get(path) {
            return *id;
        }

        self.file_path_to_id.insert(path.clone(), self.cur_file_id);
        self.file_id_to_path.insert(self.cur_file_id, path.clone());
        self.cur_file_id = self.cur_file_id.wrapping_add(1);

        self.cur_file_id.wrapping_sub(1)
    }

    fn load_page(&mut self, file_id: FileId, page_id: PageId) -> FrameId {
        let frame_id;
        // 1st - check that page_id is loaded in memory
        if let Some(frame) = self
            .mapping
            .get(&(((file_id as u64) << 32) | (page_id as u64)))
        {
            frame_id = frame;
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
                self.mapping
                    .remove(&(((info.file_id as u64) << 32) | (info.page_id as u64)));
            }

            let mut new_page_info = PageInfo {
                file_id,
                page_id,
                data: [0; PAGE_SIZE],
            };

            self.open_files
                .get_mut(&file_id)
                .unwrap()
                .read_at(
                    &mut new_page_info.data,
                    ((PAGE_SIZE as PageId) * page_id) as u64,
                )
                .unwrap();

            self.mapping
                .insert(((file_id as u64) << 32) | (page_id as u64), frame_id);
            self.frames[frame_id] = Frame::Page(new_page_info);
        }

        frame_id
    }

    pub fn read(&mut self, file_id: FileId, mut offset: usize, buffer: &mut [u8]) -> usize {
        let last_offset = offset + buffer.len();
        let first_page_id = (offset / PAGE_SIZE) as PageId;
        let last_page_id = (last_offset / PAGE_SIZE) as PageId;
        let mut bytes_copied = 0;

        for page_id in first_page_id..=last_page_id {
            let frame_id = self.load_page(file_id, page_id);

            // page is now guaranteed to be loaded
            if let Frame::Page(PageInfo {
                file_id,
                page_id,
                data,
            }) = &mut self.frames[frame_id]
            {
                let num_bytes = (PAGE_SIZE - (offset % PAGE_SIZE)).min(buffer.len() - bytes_copied);
                let data_to_copy = &data[(offset % PAGE_SIZE)..(offset % PAGE_SIZE) + num_bytes];
                offset += num_bytes;
                buffer[bytes_copied..bytes_copied + num_bytes].copy_from_slice(data_to_copy);
                bytes_copied += num_bytes;
            } else {
                panic!("Expected page to be loaded");
            }
        }

        bytes_copied
    }
}

pub fn page_cache_sequential_read(
    page_cache: Rc<RefCell<PageCache>>,
    file_id: FileId,
    mut start_offset: usize,
) -> SeqPageRead {
    let page_id = (start_offset / PAGE_SIZE) as PageId;
    let frame_id = page_cache.borrow_mut().load_page(file_id, page_id);

    SeqPageRead {
        file_id,
        cur_page_id: page_id,
        frame_id,
        page_cache,
        offset: start_offset,
    }
}

#[cfg(test)]
mod tests {
    use super::PageCache;
    use crate::utils::test_utils::*;
    use crate::{
        common::{Timestamp, Value},
        storage::{file::TimeDataFile, page_cache::page_cache_sequential_read},
    };
    use std::{cell::RefCell, cmp::min, fs::File, io::Write, path::PathBuf, rc::Rc, str::FromStr};

    #[test]
    fn test_read_whole_file() {
        set_up_files!(file_paths, "test.ty", "expected.ty");

        let mut page_cache = PageCache::new(10);
        let mut model = TimeDataFile::new();
        for i in 0..100000u64 {
            model.write_data_to_file_in_mem(i, i + 10);
        }
        let file_size = model.write(file_paths[0].clone());
        // start the test
        let file_id = page_cache.register_or_get_file_id(&file_paths[0]);
        assert_eq!(file_id, 0);

        let mut buffer = vec![0; file_size];
        page_cache.read(file_id, 0, &mut buffer);

        let mut new_file = File::create(&file_paths[1]).unwrap();
        new_file.write_all(&buffer);

        let data_file = TimeDataFile::read_data_file(file_paths[1].clone());

        assert_eq!(data_file.timestamps.len(), 100000);
        for i in 0..data_file.timestamps.len() {
            assert_eq!(data_file.timestamps[i], i as Timestamp);
            assert_eq!(data_file.values[i], (i + 10) as Value);
        }
    }

    #[test]
    fn test_read_sequential_whole_file() {
        set_up_files!(file_paths, "test.ty", "expected.ty");

        let mut page_cache = PageCache::new(10);
        let mut model = TimeDataFile::new();
        for i in 0..100000u64 {
            model.write_data_to_file_in_mem(i, i + 10);
        }
        let file_size = model.write(file_paths[0].clone());
        // start the test
        let file_id = page_cache.register_or_get_file_id(&file_paths[0]);
        assert_eq!(file_id, 0);

        let mut seq_read =
            page_cache_sequential_read(Rc::new(RefCell::new(page_cache)), file_id, 0);

        let mut buffer = vec![0; file_size];
        let mut bytes_read = 0;

        while bytes_read < file_size {
            // read 8 bytes at a time
            bytes_read += seq_read.read(&mut buffer[bytes_read..min(bytes_read + 8, file_size)]);
        }

        let mut new_file = File::create(&file_paths[1]).unwrap();
        new_file.write_all(&buffer);

        let data_file = TimeDataFile::read_data_file(file_paths[1].clone());

        assert_eq!(data_file.timestamps.len(), 100000);
        for i in 0..data_file.timestamps.len() {
            assert_eq!(data_file.timestamps[i], i as Timestamp);
            assert_eq!(data_file.values[i], (i + 10) as Value);
        }
    }
}
