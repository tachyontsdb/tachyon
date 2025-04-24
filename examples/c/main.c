#include <stdio.h>
#include <stdlib.h>

#include "Tachyon.h"

#define NUM_ENTRIES 10000

int main() {
    const char *db_dir = "./tachyon_db";
    const char *stream_name = "sensor_data";
    void *connection = NULL;
    void *inserter = NULL;
    void *error = NULL;
    void *query = NULL;

    // Open database connection
    uint8_t ret_code = tachyon_open(db_dir, &connection);
    if (ret_code != 0) {
        tachyon_error_print(ret_code, error);
        tachyon_error_free(ret_code, error);
        return EXIT_FAILURE;
    }

    // Create stream if it doesn't exist
    ret_code = tachyon_stream_create(connection, stream_name, TachyonValueType_Integer64, &error);
    if (ret_code != 0) {
        tachyon_error_print(ret_code, error);
        tachyon_error_free(ret_code, error);
        tachyon_close(connection);
        return EXIT_FAILURE;
    }

    // Create inserter
    ret_code = tachyon_inserter_create(connection, stream_name, &inserter);
    if (ret_code != 0) {
        tachyon_error_print(ret_code, error);
        tachyon_error_free(ret_code, error);
        tachyon_close(connection);
        return EXIT_FAILURE;
    }

    // Insert some data
    for (int i = 0; i < NUM_ENTRIES; i++) {
        TachyonTimestamp timestamp = (TachyonTimestamp) i;
        int64_t value = 100 + i;
        ret_code = tachyon_inserter_insert_integer64(inserter, timestamp, value, &error);
        if (ret_code != 0) {
            tachyon_error_print(ret_code, error);
            tachyon_error_free(ret_code, error);

            tachyon_inserter_close(inserter);
            tachyon_close(connection);
            return EXIT_FAILURE;
        }
    }

    // Flush inserter
    ret_code = tachyon_inserter_flush(inserter, &error);
    if (ret_code != 0) {
        tachyon_error_print(ret_code, error);
        tachyon_error_free(ret_code, error);

        tachyon_inserter_close(inserter);
        tachyon_close(connection);
        return EXIT_FAILURE;
    }

    // Clean up inserter
    tachyon_inserter_close(inserter);

    // Execute query: max(sensor_data)
    const char *query_str = "max(sensor_data)";
    ret_code = tachyon_query_create(connection, query_str, NULL, NULL, &query);
    if (ret_code != 0) {
        tachyon_error_print(ret_code, error);
        tachyon_error_free(ret_code, error);
        tachyon_close(connection);
        return EXIT_FAILURE;
    }

    // Process query result
    TachyonValue max_value;
    if (tachyon_query_next_scalar(query, &max_value)) {
        printf("Max value in sensor_data: %ld\n", max_value.integer64);
    } else {
        printf("No data found in sensor_data.\n");
    }

    // Clean up query
    tachyon_query_close(query);

    // Close database connection
    tachyon_close(connection);

    printf("Data successfully inserted and queried from stream: %s\n", stream_name);
    return EXIT_SUCCESS;
}
