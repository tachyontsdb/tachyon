#!/bin/bash

set -eu

gcc -lpthread -lsqlite3 -O3 -o c_benchmark.out main.c
