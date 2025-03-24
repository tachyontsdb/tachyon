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
    if (tachyon_open(db_dir, &connection) != 0) {
        fprintf(stderr, "Failed to open database\n");
        return EXIT_FAILURE;
    }
    
    // Create stream if it doesn't exist
    if (tachyon_stream_create(connection, stream_name, TachyonValueType_Integer64, &error) != 0) {
        fprintf(stderr, "Failed to create stream\n");
        tachyon_error_free(1, error);
    }
    
    // Create inserter
    if (tachyon_inserter_create(connection, stream_name, &inserter) != 0) {
        fprintf(stderr, "Failed to create inserter\n");
        tachyon_close(connection);
        return EXIT_FAILURE;
    }
    
    // Insert some data
    for (int i = 0; i < NUM_ENTRIES; i++) {
        TachyonTimestamp timestamp = (TachyonTimestamp) i;
        int64_t value = 100 + i;
        uint8_t code = tachyon_inserter_insert_integer64(inserter, timestamp, value, &error);
        if (code != 0) {
            fprintf(stderr, "Failed to insert data\n");
            tachyon_error_print(code, error);
            tachyon_error_free(1, error);

            tachyon_inserter_close(inserter);
            tachyon_close(connection);
            return EXIT_FAILURE;
        }
    }
    
    // Flush inserter
    if (tachyon_inserter_flush(inserter, &error) != 0) {
        fprintf(stderr, "Failed to flush inserter\n");
        tachyon_error_free(1, error);

        tachyon_inserter_close(inserter);
        tachyon_close(connection);
    }
    
    // Clean up inserter
    tachyon_inserter_close(inserter);
    
    // Execute query: max(sensor_data)
    const char *query_str = "max(sensor_data)";
    if (tachyon_query_create(connection, query_str, NULL, NULL, &query) != 0) {
        fprintf(stderr, "Failed to execute query\n");
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
