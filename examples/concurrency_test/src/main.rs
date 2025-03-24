use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, Utc};
use rand::Rng;
use tachyon_core::ReturnType;
use tachyon_core::{Connection, ValueType};

const TEST_DIR: &str = "concurrency_test_data";
const STREAM_NAME: &str = "test_stream";
const PARENT_ROWS: usize = 100;
const CHILD_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

const PARENT_OUTPUT_FILE: &str = "parent_output.txt";
const CHILD_OUTPUT_FILE: &str = "child_output.txt";

// Helper function to log with timestamp to a file
fn log_with_timestamp(file: &mut File, message: &str) -> Result<(), std::io::Error> {
    writeln!(
        file,
        "[{}] {}",
        Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
        message
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Create test directory if it doesn't exist
    if !Path::new(TEST_DIR).exists() {
        fs::create_dir(TEST_DIR).expect("Failed to create test directory");
    }

    if args.len() <= 1 {
        // Parent process initialization - print to console and to file
        println!("Parent process starting - will create stream and spawn child");
        // Create parent output file for the rest of the logging
        File::create(PARENT_OUTPUT_FILE)
            .expect("Failed to create parent output file")
            .write_all(
                format!(
                    "[{}] Parent process starting - will create stream and spawn child\n",
                    Local::now().format("%Y-%m-%d %H:%M:%S%.3f")
                )
                .as_bytes(),
            )
            .expect("Failed to write to parent output file");
        run_parent().expect("Parent process failed");
    } else if args[1] == "child" {
        // Child process initialization - print to console and to file
        println!("Child process starting - will read from stream");
        // Create child output file for the rest of the logging
        File::create(CHILD_OUTPUT_FILE)
            .expect("Failed to create child output file")
            .write_all(
                format!(
                    "[{}] Child process starting - will read from stream\n",
                    Local::now().format("%Y-%m-%d %H:%M:%S%.3f")
                )
                .as_bytes(),
            )
            .expect("Failed to write to child output file");
        run_child().expect("Child process failed");
    }
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    // Open the parent output file for appending
    let mut output_file = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(PARENT_OUTPUT_FILE)?;

    writeln!(
        output_file,
        "Parent process started at {:?}",
        Instant::now()
    )?;

    // Create a connection to the database
    let mut conn = Connection::new(TEST_DIR)?;

    // Create a stream for timestamp and value
    if !conn.check_stream_exists(STREAM_NAME).unwrap() {
        conn.create_stream(STREAM_NAME, ValueType::UInteger64)?;
        log_with_timestamp(
            &mut output_file,
            &format!("Parent: Created stream {}", STREAM_NAME),
        )?;
    } else {
        log_with_timestamp(
            &mut output_file,
            &format!("Parent: Using existing stream {}", STREAM_NAME),
        )?;
    }

    // Prepare an inserter to add data to the stream
    let mut inserter = conn.prepare_insert(STREAM_NAME).unwrap();

    let mut i = 1;
    let mut accum = 0;

    let start_time = Instant::now();

    let mut child = Command::new(env::current_exe()?).arg("child").spawn()?;

    loop {
        // Use timestamp_nanos_opt instead of deprecated timestamp_nanos
        let timestamp = i;
        accum += i;
        let value = accum;
        assert_eq!(value, i * (i + 1) / 2);

        inserter.insert_uinteger64(timestamp, value)?;

        if i % 10 == 0 {
            log_with_timestamp(
                &mut output_file,
                &format!("Parent: Inserted 10 rows {}. Last value: {}", i, value),
            )?;
        }

        // Small delay to avoid timestamp collisions
        // sleep(Duration::from_millis(1));
        i += 1;

        if start_time.elapsed() > Duration::from_secs(240) {
            break;
        }
    }

    inserter.flush()?;
    child.wait()?;

    log_with_timestamp(
        &mut output_file,
        &format!("Parent: Done at {:?}!", Instant::now()),
    )?;
    // Flush and close output file
    output_file.flush()?;
    Ok(())
}

fn run_child() -> Result<(), Box<dyn std::error::Error>> {
    // Open the child output file for appending
    let mut output_file = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(CHILD_OUTPUT_FILE)?;

    writeln!(output_file, "Child process started at {:?}", Instant::now())?;

    // Give the parent process a moment to create the database and stream
    let start_time = Instant::now();
    while !Path::new(TEST_DIR).exists() || start_time.elapsed() < Duration::from_millis(200) {
        if start_time.elapsed() > CHILD_WAIT_TIMEOUT {
            log_with_timestamp(
                &mut output_file,
                "Timed out waiting for database to be created",
            )?;
            return Err("Timed out waiting for database to be created".into());
        }
        sleep(Duration::from_millis(100));
    }

    // Connect to the database
    let mut conn = Connection::new(TEST_DIR)?;

    // Verify the stream exists
    if !conn.check_stream_exists(STREAM_NAME).unwrap() {
        log_with_timestamp(
            &mut output_file,
            &format!("Stream {} does not exist", STREAM_NAME),
        )?;
        return Err(format!("Stream {} does not exist", STREAM_NAME).into());
    }

    log_with_timestamp(
        &mut output_file,
        &format!(
            "Child: Successfully connected to database and found stream {}",
            STREAM_NAME
        ),
    )?;

    // Read from the stream using a query
    let mut total_read = 0;

    // Continue checking for new rows for a while
    let start_time = Instant::now();
    let mut values = Vec::new();

    loop {
        // Query all values from the stream with a timestamp range
        // Using the PromQL format that Tachyon expects
        let query_str = STREAM_NAME.to_string(); // Simple metric name without filters
        let mut query = conn.prepare_query(
            &query_str,
            Some(0),               // Start timestamp from 0
            Some(i64::MAX as u64), // End timestamp to maximum possible value
        )?;

        // Count how many values we can read
        let mut count = 0;
        while let Some(value) = query.next_vector() {
            count += 1;
            if count % 1000 == 0 {
                log_with_timestamp(
                    &mut output_file,
                    &format!("Still reading... {} records so far", count),
                )?;
            }
            if count > total_read {
                values.push(value.value.get_uinteger64());
                let length = values.len() as u64;

                assert_eq!(*values.last().unwrap(), length * (length + 1) / 2);
            }
        }

        if count > total_read {
            log_with_timestamp(
                &mut output_file,
                &format!(
                    "Child: Found {} new rows, total now {}",
                    count - total_read,
                    count
                ),
            )?;

            // Print the last few values we read
            let num_to_show = std::cmp::min(5, values.len());
            for (_i, value) in values.iter().rev().take(num_to_show).enumerate() {
                log_with_timestamp(&mut output_file, &format!("Child: Read value {}", value))?;
            }

            total_read = count;
        }

        // sleep(Duration::from_millis(100));

        if start_time.elapsed() > Duration::from_secs(240) {
            break;
        }
    }

    log_with_timestamp(
        &mut output_file,
        &format!("Child: Done at {:?}!", Instant::now()),
    )?;
    // Flush and close output file
    output_file.flush()?;

    Ok(())
}
