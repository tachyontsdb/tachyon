#include <cstdint>

#ifndef TACHYON_EXPORTS_H
#define TACHYON_EXPORTS_H

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

struct Connection;

extern Connection *tachyon_open(const char *const root_dir);

extern void tachyon_close(Connection *connection);

extern void tachyon_query(const Connection *const connection,
                          const char *const str_ptr,
                          const uint64_t *const start,
                          const uint64_t *const end);

extern void tachyon_insert(const Connection *const Connection,
                           const char *const str_ptr, uint64_t timestamp,
                           uint64_t value);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // TACHYON_EXPORTS_H
