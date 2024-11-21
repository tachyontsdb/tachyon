# Tachyon DB

A lightweight and local time-series database.

## Building
```
cargo build --locked --release
```

C/C++ headers will be included in the output directory.

## Running Lints

### Cargo Format
```
cargo fmt --all --check
```

### Clippy
```
cargo clippy --all-targets --all-features --locked --release -- -D warnings
```

## Running Tests and Benchmarks
First, unzip the `data.zip` file. This should create a `./data` directory.
```
unzip data.zip
```

### Tests
```
cargo test --locked --release
```

### Benchmarks
```
cargo bench --locked --bench <bench-name>
```

#### Flamegraphs
```
cargo bench --locked --bench <bench-name> -- --profile-time=20
```

#### Timescale DB
> Note: If running Timescale DB benchmarks, first run the following before running the timescaledb benchmark:
```
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg16
```

