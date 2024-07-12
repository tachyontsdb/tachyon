#!/bin/bash

set -euxo pipefail

ls -al ./
echo $'DONE\nDONE'
ls -al ../
echo $'DONE\nDONE'
ls -al ../../
echo $'DONE\nDONE'
ls -al ../../target/
echo $'DONE\nDONE'
ls -al ../../target/release/
echo $'DONE\nDONE'

objdump -dC ../../target/release/libtachyon.so | grep "tachyon_"
echo $'DONE\nDONE'

gcc -v -O3 -I../../include -L../../target/release/ ./test.c -ltachyon -o test.out

LD_LIBRARY_PATH=../../target/release/ ./test.out

rm -rf ./test.out ./test_db
