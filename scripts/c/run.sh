#!/bin/bash

set -eu

gcc -o test.out -I../../include -L../../target/release -ltachyon -O3 ./test.c

LD_LIBRARY_PATH=../../target/release/ ./test.out

rm -rf ./test.out ./test_db
