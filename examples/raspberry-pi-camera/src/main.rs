use anyhow::{Context, Result};
use image::{DynamicImage, GenericImageView};
use nokhwa::pixel_format::RgbFormat;
use nokhwa::utils::{CameraIndex, RequestedFormat, Resolution};
use nokhwa::{Camera, FrameFormat};
use std::thread;
use std::time::Duration;

// Calculate the average brightness of an image
fn calculate_brightness(image: &DynamicImage) -> f64 {
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
        return 0.0;
    }

    // Calculate average brightness (0-255)
    (sum as f64) / (pixel_count as f64)
}

fn main() -> Result<()> {
    println!("Initializing camera brightness monitor using libcamera...");

    // Set some reasonable default values
    let width = 640;
    let height = 480;
    let fps = 30;

    // Try to open the default camera (index 0)
    let camera_index = CameraIndex::Index(0);
    let requested_format = RequestedFormat::new::<RgbFormat>(Resolution::new(width, height));

    // Initialize the camera with libcamera backend
    let mut camera = Camera::new(camera_index, Some(requested_format))
        .context("Failed to initialize camera. Make sure libcamera is properly installed.")?;

    // Print camera information
    println!(
        "Camera info: {}",
        camera.info().context("Failed to get camera info")?
    );
    println!(
        "Camera resolution: {}",
        camera
            .resolution()
            .context("Failed to get camera resolution")?
    );

    // Open camera stream
    camera
        .open_stream()
        .context("Failed to open camera stream")?;

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
                let image = frame.decode_image::<RgbFormat>()?;

                // Calculate brightness
                let current_brightness = calculate_brightness(&image);

                // Log current brightness level
                println!("Current brightness: {:.2}", current_brightness);

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
