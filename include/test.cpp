#include "exports.h"
#include <cassert>

const uint64_t NUM_ITEMS = 1000;

int main() {
    Connection *conn = tachyon_open("test_db");

    for (uint64_t i = 0; i < NUM_ITEMS; ++i) {
        tachyon_insert(conn, "test_stream", i, i);
    }

    uint64_t start = 0;
    uint64_t end = NUM_ITEMS;

    Stmt *stmt = tachyon_prepare(conn, "test_stream", &start, &end);

    bool got_done = false;
    for (uint64_t i = 0; i < NUM_ITEMS; ++i) {
        TachyonResult result = tachyon_next_vector(stmt);
        if (result.t == TachyonResultType::TachyonResultDone) {
            got_done = true;
            break;
        } else if (result.t == TachyonResultType::TachyonResultVector) {
            assert(result.r.vector.timestamp == i);
            assert(result.r.vector.value == i);
        }
    }
    assert(got_done);

    tachyon_close(conn);
}
