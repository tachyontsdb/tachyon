# Tachyon DB

## Running Unit Tests
```
cargo test
```

## Running Clippy
```
cargo clippy --all-targets --all-features
```

## Running Benchmarks
First, unzip the `data.zip` file. This should create a `./data` directory.
```
unzip data.zip
```

Then run:
```
cargo bench --bench <bench-name>
```

To generate flamegraphs along with the benchmark:
```
cargo bench --bench <bench-name> -- --profile-time=20
```

If running Timescale DB benchmarks, first run the following before running the timescaledb benchmark:
```
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg16
```

