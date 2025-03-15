use anyhow::{anyhow, Result};
use nokhwa::pixel_format::{RgbFormat, YuyvFormat};
use nokhwa::utils::{
    CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType, Resolution,
};
use nokhwa::Camera;
use std::path::Path;
use std::thread;
use std::time::Duration;
use tachyon_core::{Connection, Timestamp, ValueType};

fn main() -> Result<()> {
    println!("Initializing camera brightness monitor using libcamera...");

    // Initialize Tachyon database connection
    let db_dir = Path::new("./data/brightness_db");
    println!("Connecting to Tachyon database at: {:?}", db_dir);

    // Create db_dir if it doesn't exist
    if !db_dir.exists() {
        std::fs::create_dir_all(db_dir)
            .map_err(|e| anyhow!("Failed to create database directory: {}", e))?;
    }

    let mut connection = Connection::new(db_dir)
        .map_err(|e| anyhow!("Failed to initialize Tachyon database connection: {}", e))?;

    // Stream name for brightness data
    let stream_name = "camera_brightness";

    // Create the stream if it doesn't exist
    if !connection.check_stream_exists(stream_name) {
        println!("Creating stream '{}' for brightness data", stream_name);
        connection
            .create_stream(stream_name, ValueType::Float64)
            .map_err(|e| anyhow!("Failed to create stream: {}", e))?;
    } else {
        println!(
            "Using existing stream '{}' for brightness data",
            stream_name
        );
    }

    // Prepare inserter for the stream
    let mut inserter = connection.prepare_insert(stream_name);
    println!("Tachyon database initialized successfully");

    // Set some reasonable default values for camera
    let width = 1296;
    let height = 972;
    let fps = 30;

    // Try to open the default camera (index 0)
    let camera_index = CameraIndex::Index(0);
    // let requested_format = RequestedFormat::new::<YuyvFormat>(RequestedFormatType::Exact(
    //     CameraFormat::new(Resolution::new(width, height), FrameFormat::MJPEG, fps),
    // ));
    let requested_format = RequestedFormat::new::<RgbFormat>(RequestedFormatType::Closest(
        CameraFormat::new(Resolution::new(width, height), FrameFormat::RAWRGB, fps),
    ));

    // Initialize the camera with libcamera backend
    let mut camera = Camera::new(camera_index, requested_format).map_err(|e| {
        anyhow!(
            "Failed to initialize camera. Make sure libcamera is properly installed: {}",
            e
        )
    })?;

    // Print camera information
    let camera_info = camera.info();
    println!("Camera info: {}", camera_info);

    let camera_resolution = camera.resolution();
    println!("Camera resolution: {}", camera_resolution);

    // Open camera stream
    camera
        .open_stream()
        .map_err(|e| anyhow!("Failed to open camera stream: {}", e))?;

    println!("Camera stream opened successfully");

    // Variables to track brightness changes
    let mut last_brightness = 0.0;
    let brightness_threshold = 10.0; // Threshold for significant brightness change

    println!("Starting brightness monitoring loop...");

    // Main monitoring loop
    loop {
        // Capture a frame
        match camera.frame() {
            Ok(frame) => {
                // Convert frame to image for processing
                let image = frame
                    .decode_image::<RgbFormat>()
                    .map_err(|e| anyhow!("Failed to decode image: {}", e))?;

                // Calculate brightness
                let current_brightness = {
                    let mut sum: u64 = 0;
                    let mut pixel_count = 0;

                    // Get dimensions of the image
                    let (width, height) = image.dimensions();

                    // Sum up all pixel values (RGB)
                    for y in 0..height {
                        for x in 0..width {
                            let pixel = image.get_pixel(x, y);
                            // Average the RGB channels for each pixel
                            sum += (pixel[0] as u64 + pixel[1] as u64 + pixel[2] as u64) / 3;
                            pixel_count += 1;
                        }
                    }

                    if pixel_count == 0 {
                        0.0
                    } else {
                        // Calculate average brightness (0-255)
                        (sum as f64) / (pixel_count as f64)
                    }
                };

                // Get current timestamp (microseconds since Unix epoch)
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as Timestamp;

                // Insert brightness data into Tachyon
                inserter.insert_float64(timestamp, current_brightness);

                // Periodically flush the data to disk
                // This ensures data is written even if the program exits unexpectedly
                if timestamp % 10 == 0 {
                    inserter.flush();
                }

                // Log current brightness level
                println!(
                    "Current brightness: {:.2} (stored to Tachyon at timestamp {})",
                    current_brightness, timestamp
                );

                // Check for significant brightness change
                let brightness_diff = (current_brightness - last_brightness).abs();
                if brightness_diff > brightness_threshold {
                    println!(
                        "Significant brightness change detected: {:.2} -> {:.2} (diff: {:.2})",
                        last_brightness, current_brightness, brightness_diff
                    );
                }

                last_brightness = current_brightness;
            }
            Err(e) => {
                eprintln!("Error capturing frame: {}", e);
            }
        }

        // Sleep for a short period to avoid excessive CPU usage
        thread::sleep(Duration::from_millis(500));
    }
}
