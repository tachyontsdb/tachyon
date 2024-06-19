use criterion::{criterion_group, criterion_main, Criterion};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{cell::RefCell, path::PathBuf, rc::Rc, str::FromStr};
use tachyon::{
    executor::{Buffer, OutputValue, VirtualMachine},
    storage::{file::TimeDataFile, page_cache::PageCache},
};

const NUM_ITEMS: u64 = 100000;
const NUM_FRAMES: usize = 512;

fn get_buffer() -> Buffer {
    let mut buffer = Buffer::new();

    buffer.add_open_read(
        &u16::to_le_bytes(0),
        &u32::to_le_bytes(0),
        &u64::to_le_bytes(0),
        &u64::to_le_bytes(NUM_ITEMS),
    );

    let index = buffer.len();
    buffer.add_fetch_vector(&u16::to_le_bytes(0), 0, 1);
    buffer.add_output_vector(0, 1);
    buffer.add_next(&u16::to_le_bytes(0), &u64::to_le_bytes(index));
    buffer.add_close_read(&u16::to_le_bytes(0));
    buffer.add_halt();

    buffer
}

fn bench_read_sequential_timestamps(mut vm: VirtualMachine) -> u64 {
    let mut res = 0;
    loop {
        let output = vm.execute_step();
        match output {
            OutputValue::Halted | OutputValue::Scalar(_) => break,
            OutputValue::Vector((timestamp, value)) => {
                res += timestamp + value;
            }
        }
    }
    assert_eq!(res, 10004850000);
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
    let buffer = Rc::new(get_buffer());
    c.bench_function(
        &format!("tachyon: read executor sequential 0-{}", NUM_ITEMS),
        |b| {
            b.iter(|| {
                bench_read_sequential_timestamps(VirtualMachine::new(
                    paths.clone(),
                    page_cache.clone(),
                    buffer.clone(),
                ));
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
