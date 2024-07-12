#!/bin/bash

set -euxo pipefail

gcc -lpthread -lsqlite3 -O3 -o c_benchmark.out main.c
