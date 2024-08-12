#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "Tachyon.h"

#define STREAM "test_stream{test=\"asdf\"}"
#define NUM_ITEMS 1000

int main(void) {
    struct TachyonConnection *connection = tachyon_open("./c_test_db");

    tachyon_stream_create(connection, STREAM, TachyonValueType_UInteger64);
    assert(tachyon_stream_check_exists(connection, STREAM));

    struct TachyonInserter *inserter = tachyon_inserter_create(connection, STREAM);

    uint64_t total_sum = 0;
    for (uint64_t i = 0; i < NUM_ITEMS; ++i) {
        total_sum += i;
        tachyon_inserter_insert_uinteger64(inserter, i, i);
    }

    tachyon_inserter_flush(inserter);
    tachyon_inserter_close(inserter);

    uint64_t start = 0;
    uint64_t end = NUM_ITEMS;
    struct TachyonQuery *query = tachyon_query_create(connection, STREAM, &start, &end);

    assert(tachyon_query_value_type(query) == TachyonValueType_UInteger64);
    assert(tachyon_query_return_type(query) == TachyonReturnType_Vector);

    uint64_t i = 0;
    struct TachyonVector vector;
    while (tachyon_query_next_vector(query, &vector)) {
        assert(vector.timestamp == i);
        assert(vector.value.uinteger64 == i);
        ++i;
    }

    tachyon_query_close(query);

    query = tachyon_query_create(connection, "sum(" STREAM ")", &start, &end);

    assert(tachyon_query_value_type(query) == TachyonValueType_UInteger64);
    assert(tachyon_query_return_type(query) == TachyonReturnType_Scalar);

    union TachyonValue value;
    tachyon_query_next_scalar(query, &value);
    assert(value.uinteger64 == total_sum);

    tachyon_query_close(query);

    tachyon_close(connection);

    return EXIT_SUCCESS;
}
