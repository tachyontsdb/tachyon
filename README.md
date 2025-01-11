# Tachyon DB

A lightweight and local time-series database.

## Requirements

* `x86_64` - Linux (`x86_64-v3`), macOS
* `aarch64` - Linux, macOS
* `riscv64` - Linux (`rv64gc`)

## Building
```
cargo build --locked --release
```

> Note: Generated C/C++ headers will be placed in the output (`./target/include`) directory.

## Running the GUI

From the `./tachyon_gui` directory:
```
npm run tauri dev
```

> Note: You must have all [Tauri prerequisites](https://tauri.app/start/prerequisites/) installed.

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
First, unzip the `./data.zip` file. This should create a `./data` directory.
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

Run the following before running the `timescaledb` benchmark:
```
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg16
```

