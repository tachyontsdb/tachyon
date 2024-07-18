#!/bin/bash

set -euxo pipefail

gcc -v -O3 -I../../include -L../../target/release/ ./test.c -ltachyon -o test.out

LD_LIBRARY_PATH=../../target/release/ ./test.out

rm -rf ./test.out ./test_db
