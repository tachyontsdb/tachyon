#ifdef __cplusplus
#include <cstdint>
#else
#include <stdbool.h>
#include <stdint.h>
#endif  // __cplusplus

#ifndef TACHYON_EXPORTS_H
#define TACHYON_EXPORTS_H

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

struct Connection;
struct Statement;

enum TachyonValueType {
    TachyonValueUnsignedInteger = (uint8_t)0,
    TachyonValueSignedInteger = (uint8_t)1,
    TachyonValueFloat = (uint8_t)2,
};

union TachyonValue {
    uint64_t unsigned_integer;
    int64_t signed_integer;
    double floating;
};

struct TachyonVector {
    uint64_t timestamp;
    union TachyonValue value;
};

extern struct Connection *tachyon_open(const char *const db_dir);

extern void tachyon_close(struct Connection *connection);

extern void tachyon_delete_stream(const struct Connection *const connection,
                                  const char *const stream);

extern void tachyon_insert(const struct Connection *const connection,
                           const char *const stream, uint64_t timestamp,
                           uint8_t value_type, union TachyonValue value);

extern void tachyon_insert_flush(const struct Connection *const connection);

extern struct Statement *tachyon_statement_prepare(
    const struct Connection *const connection, const char *const query,
    const uint64_t *const start, const uint64_t *const end, uint8_t value_type);

extern void tachyon_statement_close(struct Statement *statement);

extern bool tachyon_next_scalar(struct Statement *statement,
                                union TachyonValue *scalar);
extern bool tachyon_next_vector(struct Statement *statement,
                                struct TachyonVector *vector);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // TACHYON_EXPORTS_H
