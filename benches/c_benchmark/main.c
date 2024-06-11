#include <inttypes.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

uint64_t result = 0;

#define SEC_TO_NS(sec) ((sec) * 1000000000)

int callback(void *data, int argc, char **argv, char **azColName) {
    result += atoll(argv[0]) * atoll(argv[1]);
    return 0;
}

int main(void) {
    int rc;

    uint64_t fns, sns;
    struct timespec ts;

    sqlite3 *db;

    rc = sqlite3_open("../../tmp/bench_sql.sqlite", &db);
    if (rc != SQLITE_OK) {
        return EXIT_FAILURE;
    }

    rc = timespec_get(&ts, TIME_UTC);
    if (rc == 0) {
        return EXIT_FAILURE;
    }
    fns = SEC_TO_NS((uint64_t)ts.tv_sec) + (uint64_t)ts.tv_nsec;

    char *zerrMsg = NULL;
    rc = sqlite3_exec(db, "SELECT * FROM Item;", callback, NULL, &zerrMsg);
    if (rc != SQLITE_OK) {
        printf("Error is %s\n", zerrMsg);
    }

    rc = timespec_get(&ts, TIME_UTC);
    if (rc == 0) {
        return EXIT_FAILURE;
    }
    sns = SEC_TO_NS((uint64_t)ts.tv_sec) + (uint64_t)ts.tv_nsec;

    uint64_t elapsed = sns - fns;
    printf("Diff is %lu milliseconds\n", elapsed / 1000000ul);

    sqlite3_close(db);

    return EXIT_SUCCESS;
}
