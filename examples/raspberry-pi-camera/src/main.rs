use rascam::{Camera, Frame, SimpleCamera};
use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

// Calculate the average brightness of a frame
fn calculate_brightness(frame: &Frame) -> f64 {
    let mut sum: u64 = 0;

    // Sum all pixel values
    for pixel in frame.data.iter() {
        sum += *pixel as u64;
    }

    // Calculate average brightness (0-255)
    (sum as f64) / (frame.data.len() as f64)
}

fn main() -> Result<(), Box<dyn Error>> {
    println!("Initializing camera brightness monitor...");

    // Get list of cameras
    let cameras = Camera::new()?;

    if cameras.len() == 0 {
        return Err("No cameras found!".into());
    }

    println!("Found {} camera(s)", cameras.len());

    // Initialize the first camera
    let camera_id = 0;
    let mut camera = SimpleCamera::new(cameras[camera_id].clone())?;

    // Configure camera
    camera.activate()?;
    let info = camera.info()?;
    println!("Camera info: {:?}", info);

    // Set some reasonable default values
    let width = 640;
    let height = 480;

    // Variables to track brightness changes
    let mut last_brightness = 0.0;
    let brightness_threshold = 10.0; // Threshold for significant brightness change

    println!("Starting brightness monitoring loop...");

    // Main monitoring loop
    loop {
        // Capture a frame
        match camera.capture() {
            Ok(frame) => {
                let current_brightness = calculate_brightness(&frame);

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
