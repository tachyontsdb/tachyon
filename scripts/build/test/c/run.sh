#!/bin/bash

set -euxo pipefail

gcc -v -I../../../../target/include/ -L../../../../target/debug/ ./c_test.c -ltachyon_core -o c_test.out

LD_LIBRARY_PATH=../../../../target/debug/ ./c_test.out

rm -rf ./c_test.out ./c_test_db/
