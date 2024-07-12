#include <assert.h>
#include <stdio.h>

#include "TachyonDB.h"

const uint64_t NUM_ITEMS = 1000;

int main() {
    struct Connection *connection = tachyon_open("test_db");

    uint64_t total_sum = 0;
    for (uint64_t i = 0; i < NUM_ITEMS; ++i) {
        total_sum += i;

        union TachyonValue value;
        value.unsigned_integer = i;
        tachyon_insert(connection, "test_stream{test=\"asdf\"}", i, value);
    }

    tachyon_insert_flush(connection);

    uint64_t start = 0;
    uint64_t end = NUM_ITEMS;

    struct Stmt *stmt =
        tachyon_statement_prepare(connection, "test_stream{test=\"asdf\"}",
                                  &start, &end, TachyonValueUnsignedInteger);

    uint64_t i = 0;
    struct TachyonVector vector;
    while (tachyon_next_vector(stmt, &vector)) {
        assert(vector.timestamp == i);
        assert(vector.value.unsigned_integer == i);

        printf("Timestamp: %lu\n", vector.timestamp);

        ++i;
    }

    tachyon_statement_close(stmt);

    stmt =
        tachyon_statement_prepare(connection, "sum(test_stream{test=\"asdf\"})",
                                  &start, &end, TachyonValueUnsignedInteger);

    union TachyonValue value;
    tachyon_next_scalar(stmt, &value);
    assert(value.unsigned_integer == total_sum);
    printf("Sum: %lu\n", value.unsigned_integer);

    tachyon_statement_close(stmt);

    tachyon_close(connection);
}
