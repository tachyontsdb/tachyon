use super::hash_map::IDLookup;
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::rc::Rc;

pub type FileId = u32;

type PageId = u32;
type FrameId = usize;

const FILE_SIZE: usize = 1_000_000;
const PAGE_SIZE: usize = 4_096;

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

#[derive(Clone, Copy, Default)]
#[repr(transparent)]
pub struct FastNoHash(u64);

impl Hasher for FastNoHash {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        panic!("No bytes please!");
    }

    fn write_u32(&mut self, i: u32) {
        self.0 = i.into();
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

pub struct SeqPageRead {
    page_cache: Rc<RefCell<PageCache>>,
    file_id: FileId,
    cur_page_id: PageId,
    frame_id: FrameId,
    offset: usize,
}

impl Read for SeqPageRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut page_cache = self.page_cache.borrow_mut();

        let mut bytes_copied = 0;
        while bytes_copied < buf.len() {
            // Make sure correct page is in the frame (not evicted)
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
                    (PAGE_SIZE - (self.offset % PAGE_SIZE)).min(buf.len() - bytes_copied);
                let data_to_copy =
                    &data[(self.offset % PAGE_SIZE)..(self.offset % PAGE_SIZE) + num_bytes];
                self.offset += num_bytes;
                buf[bytes_copied..bytes_copied + num_bytes].copy_from_slice(data_to_copy);
                bytes_copied += num_bytes;

                if self.offset % PAGE_SIZE == 0 {
                    self.cur_page_id += 1;
                }
            }
        }

        Ok(bytes_copied)
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

impl PageCache {
    pub fn new(num_frames: usize) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        for _ in 0..num_frames {
            frames.push(Frame::Empty);
        }

        Self {
            frames,
            mapping: IDLookup::new_with_capacity(2 * num_frames),
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
        self.cur_file_id += 1;

        self.cur_file_id - 1
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
            // Check that file is open
            if let std::collections::hash_map::Entry::Vacant(e) = self.open_files.entry(file_id) {
                let path = self.file_id_to_path.get(&file_id).unwrap();
                e.insert(File::open(path).unwrap());
            }

            // Find next available frame
            // TODO: Change eviction scheme
            frame_id = self.root_free;
            self.root_free = (self.root_free + 1) % self.frames.len();

            // If there was a page there, remove it from mapping
            if let Frame::Page(info) = &self.frames[frame_id] {
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

            // Page is now guaranteed to be loaded
            if let Frame::Page(PageInfo { data, .. }) = &self.frames[frame_id] {
                let num_bytes = (PAGE_SIZE - (offset % PAGE_SIZE)).min(buffer.len() - bytes_copied);
                let data_to_copy = &data[(offset % PAGE_SIZE)..(offset % PAGE_SIZE) + num_bytes];
                offset += num_bytes;
                buffer[bytes_copied..bytes_copied + num_bytes].copy_from_slice(data_to_copy);
                bytes_copied += num_bytes;
            } else {
                panic!("Expected page to be loaded!");
            }
        }

        bytes_copied
    }
}

pub fn page_cache_sequential_read(
    page_cache: Rc<RefCell<PageCache>>,
    file_id: FileId,
    start_offset: usize,
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
    use super::{page_cache_sequential_read, PageCache};
    use crate::storage::file::TimeDataFile;
    use crate::utils::test::*;
    use crate::{StreamId, Timestamp, ValueType, Version};
    use std::cell::RefCell;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::rc::Rc;

    #[test]
    fn test_read_whole_file() {
        set_up_files!(file_paths, "test.ty", "expected.ty");

        let mut page_cache = PageCache::new(10);
        let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
        for i in 0..100000u64 {
            model.write_data_to_file_in_mem(i, (i + 10).into());
        }
        let file_size = model.write(file_paths[0].clone());
        // Start the test
        let file_id = page_cache.register_or_get_file_id(&file_paths[0]);
        assert_eq!(file_id, 0);

        let mut buffer = vec![0; file_size];
        page_cache.read(file_id, 0, &mut buffer);

        let mut new_file = File::create(&file_paths[1]).unwrap();
        new_file.write_all(&buffer).unwrap();

        let data_file = TimeDataFile::read_data_file(file_paths[1].clone());

        assert_eq!(data_file.timestamps.len(), 100000);
        for i in 0..data_file.timestamps.len() {
            assert_eq!(data_file.timestamps[i], i as Timestamp);
            assert!(data_file.values[i].eq_same(ValueType::UInteger64, &((i + 10) as u64).into()));
        }
    }

    #[test]
    fn test_read_sequential_whole_file() {
        set_up_files!(file_paths, "test.ty", "expected.ty");

        let mut page_cache = PageCache::new(10);
        let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
        for i in 0..100000u64 {
            model.write_data_to_file_in_mem(i, (i + 10).into());
        }
        let file_size = model.write(file_paths[0].clone());
        // Start the test
        let file_id = page_cache.register_or_get_file_id(&file_paths[0]);
        assert_eq!(file_id, 0);

        let mut seq_read =
            page_cache_sequential_read(Rc::new(RefCell::new(page_cache)), file_id, 0);

        let mut buffer = vec![0; file_size];
        let mut bytes_read = 0;

        while bytes_read < file_size {
            // Read 8 bytes at a time
            bytes_read += seq_read
                .read(&mut buffer[bytes_read..(bytes_read + 8).min(file_size)])
                .unwrap();
        }

        let mut new_file = File::create(&file_paths[1]).unwrap();
        new_file.write_all(&buffer).unwrap();

        let data_file = TimeDataFile::read_data_file(file_paths[1].clone());

        assert_eq!(data_file.timestamps.len(), 100000);
        for i in 0..data_file.timestamps.len() {
            assert_eq!(data_file.timestamps[i], i as Timestamp);
            assert!(data_file.values[i].eq_same(ValueType::UInteger64, &((i + 10) as u64).into()));
        }
    }
}
