use anyhow::{anyhow, Result};
use image::{ImageBuffer, Rgb};
use libcamera::{
    Camera, CameraConfiguration, CameraManager, FrameBuffer, FrameBufferAllocator, PixelFormat,
    Rectangle, Stream,
};
use std::path::Path;
use std::sync::Arc;
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
    let width = 640;
    let height = 480;

    // Initialize the libcamera manager
    let manager =
        CameraManager::new().map_err(|e| anyhow!("Failed to initialize camera manager: {}", e))?;

    // Start the camera manager
    manager
        .start()
        .map_err(|e| anyhow!("Failed to start camera manager: {}", e))?;

    // Get available cameras
    let cameras = manager.cameras();
    if cameras.is_empty() {
        return Err(anyhow!("No cameras available"));
    }

    println!("Found {} camera(s)", cameras.len());
    for (i, camera) in cameras.iter().enumerate() {
        println!("Camera {}: {}", i, camera.id());
    }

    // Acquire the first camera
    let camera = cameras[0]
        .acquire()
        .map_err(|e| anyhow!("Failed to acquire camera: {}", e))?;
    println!("Using camera: {}", camera.id());

    // Configure the camera
    let mut config = camera
        .generate_configuration(&[Rectangle::new(0, 0, width, height)])
        .map_err(|e| anyhow!("Failed to generate camera configuration: {}", e))?;

    // Configure streams - in this case we have only one stream (index 0)
    let stream_config = config.get_stream_config_mut(0);
    stream_config.set_pixel_format(PixelFormat::new(b"RGB3")); // RGB24 format
    stream_config.set_size(width, height);

    // Validate and apply the configuration
    config
        .validate()
        .map_err(|e| anyhow!("Failed to validate camera configuration: {}", e))?;
    config = camera
        .configure(config)
        .map_err(|e| anyhow!("Failed to configure camera: {}", e))?;

    // Create a frame buffer allocator
    let allocator = FrameBufferAllocator::new(&camera);

    // Allocate frame buffers for our stream
    let stream = config.get_stream(0);
    let mut buffers = allocator
        .alloc(stream)
        .map_err(|e| anyhow!("Failed to allocate frame buffers: {}", e))?;

    // Start the camera
    camera
        .start()
        .map_err(|e| anyhow!("Failed to start camera: {}", e))?;
    println!("Camera started successfully");

    // Queue all available buffers
    for buffer in &buffers {
        camera
            .queue_request(buffer)
            .map_err(|e| anyhow!("Failed to queue request: {}", e))?;
    }

    // Variables to track brightness changes
    let mut last_brightness = 0.0;
    let brightness_threshold = 10.0; // Threshold for significant brightness change

    println!("Starting brightness monitoring loop...");

    // Main monitoring loop
    for _ in 0..100 {
        // Capture 100 frames for testing
        // Wait for a completed request (this blocks until a frame is available)
        let request = camera
            .get_completed_request()
            .map_err(|e| anyhow!("Failed to get completed request: {}", e))?;

        // Get the frame buffer from the request
        let buffer = request
            .get_buffer(stream)
            .map_err(|e| anyhow!("Failed to get buffer from request: {}", e))?;

        // Get the buffer data
        let planes = buffer.get_planes();
        if planes.is_empty() {
            return Err(anyhow!("No planes in buffer"));
        }

        // Get the first plane (for RGB format, there's typically only one plane)
        let plane = &planes[0];
        let data = plane.data();

        // Create an RGB image from the buffer data
        // The data layout is expected to be RGB24 (3 bytes per pixel)
        let image: ImageBuffer<Rgb<u8>, Vec<u8>> =
            match ImageBuffer::from_raw(width, height, data.to_vec()) {
                Some(img) => img,
                None => return Err(anyhow!("Failed to create image from buffer data")),
            };

        // Calculate brightness
        let current_brightness = {
            let mut sum: u64 = 0;
            let mut pixel_count = 0;

            // Sum up all pixel values (RGB)
            for (_, _, pixel) in image.enumerate_pixels() {
                // Average the RGB channels for each pixel
                sum += (pixel[0] as u64 + pixel[1] as u64 + pixel[2] as u64) / 3;
                pixel_count += 1;
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

        // Queue the buffer again for reuse
        camera
            .queue_request(&[buffer])
            .map_err(|e| anyhow!("Failed to requeue buffer: {}", e))?;

        // Sleep for a short period to avoid excessive CPU usage
        thread::sleep(Duration::from_millis(500));
    }

    // Stop the camera
    camera
        .stop()
        .map_err(|e| anyhow!("Failed to stop camera: {}", e))?;

    // Flush any remaining data
    inserter.flush();

    Ok(())
}
