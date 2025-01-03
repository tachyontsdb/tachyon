use criterion::{criterion_group, criterion_main, Criterion};
use postgres::{Client, NoTls};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};

// TODO: Add black_box

const NUM_ITEMS: u64 = 100000;

fn bench_read_timescale(client: &mut Client) -> u64 {
    let mut res = 0;
    let query = "select timestamp, value from item;";
    for row in client.query(query, &[]).unwrap() {
        let timestamp: i64 = row.get(0);
        let value: i64 = row.get(1);
        res += timestamp + value;
    }
    res as u64
}

fn criterion_benchmark(c: &mut Criterion) {
    // set up TimescaleDB benchmark
    let mut client = Client::connect(
        "host=localhost user=postgres password=password dbname=postgres",
        NoTls,
    )
    .unwrap();

    client.execute("Drop table if exists Item;", &[]).unwrap();

    // Example query
    let mut transaction = client.transaction().unwrap();
    transaction
        .execute(
            "
    CREATE TABLE Item (
        timestamp bigint,
        value bigint
    )
    ",
            &[],
        )
        .unwrap();
    transaction
        .execute(
            "
            SELECT create_hypertable('item', by_range('timestamp'));
    ",
            &[],
        )
        .unwrap();

    for i in 0..NUM_ITEMS {
        let timestamp = i;
        let value = i + (i % 100);
        transaction
            .execute(
                "Insert into Item values ($1, $2)",
                &[&(timestamp as i64), &(value as i64)],
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    // Execute the query and process results
    c.bench_function(
        &format!("Timescale DB: read sequential 0-{}", NUM_ITEMS),
        |b| b.iter(|| bench_read_timescale(&mut client)),
    );
    client.execute("drop table item;", &[]).unwrap();
}

fn get_config() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = criterion_benchmark
);
criterion_main!(benches);
