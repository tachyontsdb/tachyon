#!/bin/bash

set -euxo pipefail

rm -rf ./tmp
mkdir ./tmp

cargo run --release ./tmp csv ./data/input_web_dataset.csv "input{ty = \"web\"}"
cargo run --release ./tmp csv ./data/input_mobile_dataset.csv "input{ty = \"mobile\"}"

cargo run --release ./tmp
