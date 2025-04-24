#!/bin/bash

set -euxo pipefail

cargo build --locked --release

make

LD_LIBRARY_PATH=../../target/release/ ./main