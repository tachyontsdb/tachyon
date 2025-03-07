use criterion::{criterion_group, criterion_main, Criterion};
use std::{hint::black_box, path::PathBuf, str::FromStr};
use tachyon_core::tachyon_benchmarks::PageCache;

fn bench_page_cache_init() -> u64 {
    let _ = black_box(PageCache::new(black_box(10000)));
    0
}

fn bench_page_cache_hash(strings: &[PathBuf]) -> u64 {
    let mut page_cache = black_box(PageCache::new(black_box(100)));
    let mut res = 0;

    for path in strings {
        res += black_box(page_cache.register_or_get_file_id(black_box(path)));
    }

    res as u64
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("page cache init", |b| b.iter(bench_page_cache_init));

    let mut test = Vec::<PathBuf>::with_capacity(100000);
    for i in 0..100000 {
        test.push(
            PathBuf::from_str(&(String::from_str("srihari").unwrap() + &i.to_string().to_owned()))
                .unwrap(),
        )
    }

    c.bench_function("page cache hash", |b| {
        b.iter(|| bench_page_cache_hash(&test))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
