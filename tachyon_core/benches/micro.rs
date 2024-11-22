use criterion::{criterion_group, criterion_main, Criterion};

fn bench_float_parsing(floats: &[[u8; 8]]) -> f64 {
    let mut res = 0.0;

    for float in floats {
        let cur = ((float[0] as u64) << 56)
            | ((float[1] as u64) << 48)
            | ((float[2] as u64) << 40)
            | ((float[3] as u64) << 32)
            | ((float[4] as u64) << 24)
            | ((float[5] as u64) << 16)
            | ((float[6] as u64) << 8)
            | (float[7] as u64);
        res += f64::from_bits(cur);
    }
    res
}

fn bench_float_parsing_2(floats: &[u64]) -> f64 {
    let mut res = 0.0;

    for cur in floats {
        res += f64::from_bits(*cur);
    }
    res
}

fn bench_float_parsing_3(floats: &[[u8; 8]]) -> f64 {
    let mut res = 0.0;

    for float in floats {
        res += f64::from_be_bytes(*float);
    }
    res
}

fn float_baseline(floats: &[f64]) -> f64 {
    let mut res = 0.0;

    for float in floats {
        res += float;
    }
    res
}

fn int_baseline(floats: &[u64]) -> u64 {
    let mut res = 0;

    for cur in floats {
        res += cur;
    }
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut floats = Vec::new();
    for i in 0..8 * 1000000 as u64 {
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

    c.bench_function("float parsing 1", |b| {
        b.iter(|| bench_float_parsing(&floats));
    });

    c.bench_function("float parsing 2", |b| {
        b.iter(|| bench_float_parsing_2(&u64s));
    });

    c.bench_function("float parsing 3", |b| {
        b.iter(|| bench_float_parsing_3(&floats));
    });

    c.bench_function("int64 baseline", |b| {
        b.iter(|| int_baseline(&u64s));
    });

    c.bench_function("f64 baseline", |b| {
        b.iter(|| float_baseline(&actual_floats));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
