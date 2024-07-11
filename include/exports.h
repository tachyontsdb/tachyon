#include <cstdint>

#ifndef TACHYON_EXPORTS_H
#define TACHYON_EXPORTS_H

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

struct Connection;
struct Stmt;

enum TachyonResultType : uint8_t {
    TachyonResultDone,
    TachyoResultnScalar,
    TachyonResultVector,
};

enum TachyonVectorValueType : uint64_t {
    UnsignedInteger,
    SignedInteger,
    Float,
};

struct TachyonResult {
    TachyonResultType t;
    union {
        uint64_t scalar;
        struct {
            uint64_t timestamp;
            TachyonVectorValueType t;
            union {
                uint64_t u;
                int64_t s;
                double d;
            } value;
        } vector;
    } r;
};

extern struct Connection *tachyon_open(const char *const root_dir);

extern void tachyon_close(struct Connection *connection);

extern Stmt *tachyon_prepare(const struct Connection *const connection,
                           const char *const str_ptr,
                           const uint64_t *const start,
                           const uint64_t *const end);

extern struct TachyonResult tachyon_next_vector(struct Stmt *stmt);

extern void tachyon_insert(const struct Connection *const Connection,
                           const char *const str_ptr, uint64_t timestamp,
                           uint64_t value);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // TACHYON_EXPORTS_H
