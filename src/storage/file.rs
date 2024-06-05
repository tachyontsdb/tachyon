use std::{fs::File, io::Read, io::Write, path::PathBuf};

const MAGIC: [u8; 4] = [b'T', b'a', b'c', b'h'];

pub type Timestamp = u64;
pub type Value = u32;

#[derive(Default, Debug)]
#[repr(C)]
struct Header {
    magic: [u8; 4],
    version: u16,

    stream_id: u64,

    min_timestamp: Timestamp,
    max_timestamp: Timestamp,
    value_sum: u64,
    count: u32,
    min_value: Value,
    max_value: Value,
}

#[derive(Debug)]
#[repr(C)]
pub struct TimeDataFile {
    header: Header,
    timestamps: Vec<Timestamp>,
    values: Vec<Value>,
}

impl TimeDataFile {
    pub fn new() -> Self {
        Self {
            header: Header {
                magic: MAGIC,
                ..Header::default()
            },
            timestamps: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn read_data_file(path: PathBuf) -> Self {
        use std::mem::{size_of, transmute};

        // TODO: remove unwraps
        let mut file = File::open(path).unwrap();

        let header: Header;
        let mut buf: [u8; size_of::<Header>()] = [0x00; size_of::<Header>()];
        file.read_exact(&mut buf).unwrap();
        header = unsafe { transmute::<[u8; size_of::<Header>()], Header>(buf) };

        let mut buf: [u8; size_of::<Timestamp>()] = [0x00; size_of::<Timestamp>()];
        let mut timestamps = Vec::<Timestamp>::new();
        for _ in 0..header.count {
            file.read_exact(&mut buf).unwrap();
            timestamps.push(Timestamp::from_le_bytes(buf));
        }

        let mut buf: [u8; size_of::<Value>()] = [0x00; size_of::<Value>()];
        let mut values = Vec::<Value>::new();
        for _ in 0..header.count {
            file.read_exact(&mut buf).unwrap();
            values.push(Value::from_le_bytes(buf));
        }

        Self {
            header,
            timestamps,
            values,
        }
    }

    pub fn write(&self, path: PathBuf) {
        use std::mem::{size_of, transmute};

        let mut file = File::create(path).unwrap();

        // write header
        let buf = unsafe { transmute::<&Header, &[u8; size_of::<Header>()]>(&self.header) };
        file.write_all(buf).unwrap();

        // write timestamps
        for i in 0..self.header.count {
            file.write_all(&self.timestamps[i as usize].to_le_bytes()).unwrap();
        }

        // write values
        for i in 0..self.header.count {
            file.write_all(&self.values[i as usize].to_le_bytes()).unwrap();
        }
    }


    pub fn write_data_to_file_in_mem(&mut self, timestamp: Timestamp, value: Value) {
        self.header.count += 1;
        self.header.value_sum += u64::from(value);
        
        self.header.max_timestamp = u64::max(self.header.max_timestamp, timestamp);
        self.header.min_timestamp = u64::min(self.header.min_timestamp, timestamp);
        
        self.header.max_value = u32::max(self.header.max_value, value);
        self.header.min_value = u32::min(self.header.min_value, value);

        self.timestamps.push(timestamp);
        self.values.push(value);

        println!("After update: {:#?}", self)
    }
}

// #[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write() {
        let mut model = TimeDataFile::new();
        for i in 0..10u32 {
            model.write_data_to_file_in_mem(i.into(), i + 10);
        }

        model.write("./cool.ty".into());
    }

    #[test]
    fn test_read() {
        let model = TimeDataFile::read_data_file("./cool.ty".into());
        assert_eq!(model.header.count, 10);
        assert_eq!(model.timestamps[0], 0);
        assert_eq!(model.values[0], 10);
        println!("FILE: {:#?}", model);
    }
}
