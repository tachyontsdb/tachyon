use criterion::{criterion_group, criterion_main, Criterion};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{cell::RefCell, path::PathBuf, rc::Rc, str::FromStr};
use tachyon::{
    executor::{execute, Context, OperationCode, OutputValue},
    storage::{file::TimeDataFile, page_cache::PageCache},
};

const NUM_ITEMS: u64 = 100000;
const NUM_FRAMES: usize = 512;

fn get_buffer() -> Vec<u8> {
    let mut buffer = vec![OperationCode::Init as u8];

    buffer.push(OperationCode::OpenRead as u8);
    buffer.extend_from_slice(&u64::to_le_bytes(0));
    buffer.extend_from_slice(&u64::to_le_bytes(0));
    buffer.extend_from_slice(&u64::to_le_bytes(0));
    buffer.extend_from_slice(&u64::to_le_bytes(NUM_ITEMS));

    buffer.push(OperationCode::FetchVector as u8);
    buffer.extend_from_slice(&u64::to_le_bytes(0));
    buffer.extend_from_slice(&u64::to_le_bytes(0));
    buffer.extend_from_slice(&u64::to_le_bytes(1));

    buffer.push(OperationCode::OutputVector as u8);
    buffer.extend_from_slice(&u64::to_le_bytes(0));
    buffer.extend_from_slice(&u64::to_le_bytes(1));

    buffer.push(OperationCode::Next as u8);
    buffer.extend_from_slice(&u64::to_le_bytes(0));
    buffer.extend_from_slice(&u64::to_le_bytes(34));

    buffer.push(OperationCode::CloseRead as u8);
    buffer.extend_from_slice(&u64::to_le_bytes(0));

    buffer.push(OperationCode::Halt as u8);

    buffer
}

fn bench_read_sequential_timestamps(mut context: Context, buffer: &[u8]) -> u64 {
    execute(&mut context, buffer);

    let mut res = 0;
    // while let Some(OutputValue::Vector((timestamp, value))) = context.get_output() {
    //
    // }
    for item in context.outputs {
        if let OutputValue::Vector((timestamp, value)) = item {
            res += timestamp + value;
        }
    }
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    let paths: Rc<[Rc<[PathBuf]>]> = Rc::new([Rc::new([PathBuf::from_str(
        "./tmp/bench_sequential_read.ty",
    )
    .unwrap()])]);

    let mut model = TimeDataFile::new();
    for i in 0..NUM_ITEMS {
        model.write_data_to_file_in_mem(i, i + (i % 100));
    }
    model.write(paths[0][0].clone());

    let page_cache = Rc::new(RefCell::new(PageCache::new(NUM_FRAMES)));
    let buffer = get_buffer();
    c.bench_function(
        &format!("tachyon: read executor sequential 0-{}", NUM_ITEMS),
        |b| {
            b.iter(|| {
                bench_read_sequential_timestamps(
                    Context::new(paths.clone(), page_cache.clone()),
                    &buffer,
                );
            });
        },
    );
}

fn get_config() -> Criterion {
    let options = Options::default();
    Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = criterion_benchmark
);
criterion_main!(benches);
