use std::ptr;

use criterion::{criterion_group, criterion_main, Criterion};

pub type Timestamp = u64;
pub type Value = u64;

#[derive(PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum TachyonValueType {
    UnsignedInteger,
    SignedInteger,
    Float,
}

#[repr(C)]
pub union TachyonValue {
    pub unsigned_integer: u64,
    pub signed_integer: i64,
    pub floating: f64,
}

struct TestData {
    u64s: Vec<u64>,
    floats: Vec<f64>,
    i: usize,
}

impl TestData {
    fn new() -> Self {
        let mut floats = Vec::new();
        for i in 0..8 * 10000000 as u64 {
            floats.push([
                i as u8,
                (i + 1) as u8,
                (i + 2) as u8,
                (i + 3) as u8,
                (i + 4) as u8,
                (i + 5) as u8,
                (i + 6) as u8,
                (i + 7) as u8,
            ]);
        }

        let mut u64s = Vec::new();
        for float in &floats {
            let cur = ((float[0] as u64) << 56)
                | ((float[1] as u64) << 48)
                | ((float[2] as u64) << 40)
                | ((float[3] as u64) << 32)
                | ((float[4] as u64) << 24)
                | ((float[5] as u64) << 16)
                | ((float[6] as u64) << 8)
                | (float[7] as u64);
            u64s.push(cur);
        }

        let mut actual_floats = Vec::new();
        for i in &u64s {
            actual_floats.push(f64::from_bits(*i));
        }

        TestData {
            u64s,
            floats: actual_floats,
            i: 0,
        }
    }
    fn get_next_float(&mut self) -> Option<TachyonValue> {
        if self.i >= self.floats.len() {
            return None;
        }
        self.i += 1;
        Some(TachyonValue {
            floating: self.floats[self.i - 1],
        })
    }

    fn get_next_u64(&mut self) -> Option<TachyonValue> {
        if self.i >= self.floats.len() {
            return None;
        }
        self.i += 1;
        Some(TachyonValue {
            unsigned_integer: self.u64s[self.i - 1],
        })
    }
    fn reset(&mut self) {
        self.i = 0;
    }
}

struct TestData2 {
    u64s: Vec<u64>,
    floats: Vec<f64>,
    i: usize,
}

impl TestData2 {
    fn new() -> Self {
        let mut floats = Vec::new();
        for i in 0..8 * 10000000 as u64 {
            floats.push([
                i as u8,
                (i + 1) as u8,
                (i + 2) as u8,
                (i + 3) as u8,
                (i + 4) as u8,
                (i + 5) as u8,
                (i + 6) as u8,
                (i + 7) as u8,
            ]);
        }

        let mut u64s = Vec::new();
        for float in &floats {
            let cur = ((float[0] as u64) << 56)
                | ((float[1] as u64) << 48)
                | ((float[2] as u64) << 40)
                | ((float[3] as u64) << 32)
                | ((float[4] as u64) << 24)
                | ((float[5] as u64) << 16)
                | ((float[6] as u64) << 8)
                | (float[7] as u64);
            u64s.push(cur);
        }

        let mut actual_floats = Vec::new();
        for i in &u64s {
            actual_floats.push(f64::from_bits(*i));
        }

        TestData2 {
            u64s,
            floats: actual_floats,
            i: 0,
        }
    }
    fn get_next_float(&mut self) -> *const u8 {
        if self.i >= self.floats.len() {
            return ptr::null();
        }
        self.i += 1;
        (&self.floats[self.i - 1] as *const f64) as *const u8
    }

    fn get_next_u64(&mut self) -> *const u8 {
        if self.i >= self.u64s.len() {
            return ptr::null();
        }
        self.i += 1;
        (&self.u64s[self.i - 1] as *const u64) as *const u8
    }

    fn reset(&mut self) {
        self.i = 0;
    }
}

fn bench_test_data_float_1(test_data: &mut TestData) -> f64 {
    let mut res = 0.0;

    loop {
        let value = test_data.get_next_float();
        match value {
            Some(value) => res += unsafe { value.floating },
            None => break,
        };
    }
    test_data.reset();
    res
}

fn bench_test_data_float_2(test_data: &mut TestData2) -> f64 {
    let mut res = 0.0;

    loop {
        let value = test_data.get_next_float();
        if value.is_null() {
            break;
        }
        res += unsafe { *(value as *const f64) };
    }
    test_data.reset();
    res
}

fn bench_test_data_u64_1(test_data: &mut TestData) -> u64 {
    let mut res = 0;

    loop {
        let value = test_data.get_next_u64();
        match value {
            Some(value) => res += unsafe { value.unsigned_integer },
            None => break,
        };
    }
    test_data.reset();
    res
}

fn bench_test_data_u64_2(test_data: &mut TestData2) -> u64 {
    let mut res = 0;

    loop {
        let value = test_data.get_next_u64();
        if value.is_null() {
            break;
        }
        res += unsafe { *(value as *const u64) };
    }
    test_data.reset();
    res
}

fn bench_add_1(test_data_1: &mut TestData, test_data_2: &mut TestData) -> f64 {
    let mut res = 0.0;
    loop {
        let value = test_data_1.get_next_float();
        let value_2 = test_data_2.get_next_float();
        if value.is_none() {
            break;
        }
        res += unsafe { value.unwrap().floating + value_2.unwrap().floating };
    }
    test_data_1.reset();
    test_data_2.reset();
    res
}

fn bench_add_2(test_data_1: &mut TestData2, test_data_2: &mut TestData2) -> f64 {
    let mut res = 0.0;
    loop {
        let value = test_data_1.get_next_float();
        let value_2 = test_data_2.get_next_float();
        if value.is_null() {
            break;
        }
        res += unsafe { *(value as *const f64) + *(value_2 as *const f64) };
    }
    test_data_1.reset();
    test_data_2.reset();
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut test_data_1 = TestData::new();

    let mut test_data_2 = TestData2::new();
    c.bench_function("float parsing 1", |b| {
        b.iter(|| bench_test_data_float_1(&mut test_data_1));
    });

    c.bench_function("float parsing 2", |b| {
        b.iter(|| bench_test_data_float_2(&mut test_data_2));
    });

    c.bench_function("int parsing 1", |b| {
        b.iter(|| bench_test_data_u64_1(&mut test_data_1));
    });

    c.bench_function("int parsing 2", |b| {
        b.iter(|| bench_test_data_u64_2(&mut test_data_2));
    });

    let mut test_data_12 = TestData::new();
    c.bench_function("add 1", |b| {
        b.iter(|| bench_add_1(&mut test_data_1, &mut test_data_12));
    });

    let mut test_data_22 = TestData2::new();
    c.bench_function("add 2", |b| {
        b.iter(|| bench_add_2(&mut test_data_2, &mut test_data_22));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
