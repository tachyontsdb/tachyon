#!/bin/bash

set -euxo pipefail

gcc -v -I../../include -L../../target/debug/ ./test.c -ltachyon -o test.out

LD_LIBRARY_PATH=../../target/debug/ ./test.out

rm -rf ./test.out ./test_db
