use anyhow::{anyhow, Result};
use libcamera::{
    camera::CameraConfigurationStatus,
    camera_manager::CameraManager,
    framebuffer::AsFrameBuffer,
    framebuffer_allocator::{FrameBuffer, FrameBufferAllocator},
    framebuffer_map::MemoryMappedFrameBuffer,
    pixel_format::PixelFormat,
    properties,
    request::ReuseFlag,
    stream::StreamRole,
};
use std::{path::Path, time::{Instant, SystemTime, UNIX_EPOCH}};
use std::sync::Arc;
use std::thread;
use std::{fs::OpenOptions, io::Write, process::exit, time::Duration};
use tachyon_core::{Connection, Timestamp, ValueType};

// Since your camera supports only YUYV, we define the pixel format for YUYV.
// Note: While the constant name below is PIXEL_FORMAT_YUYV, you can rename it as needed.
const PIXEL_FORMAT_YUYV: PixelFormat =
    PixelFormat::new(u32::from_le_bytes([b'Y', b'U', b'Y', b'V']), 0);

fn main() -> Result<()> {
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

    // Get the output filename from the command-line arguments.
    let filename = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Error: missing file output parameter");
        eprintln!("Usage: ./video_capture </path/to/output.yuyv>");
        exit(1);
    });

    // Initialize the CameraManager and get the first available camera.
    let mgr = CameraManager::new().expect("Failed to create CameraManager");
    let cameras = mgr.cameras();
    let cam = cameras.get(0).expect("No cameras found");
    let mut cam = cam.acquire().expect("Unable to acquire camera");

    // Generate a default configuration for video recording.
    let mut cfgs = cam
        .generate_configuration(&[StreamRole::VideoRecording])
        .expect("Failed to generate configuration");

    // Set the desired pixel format to YUYV.
    let mut stream_cfg = cfgs.get_mut(0).expect("No stream configuration found");
    stream_cfg.set_pixel_format(PIXEL_FORMAT_YUYV);

    println!("Generated configuration: {:#?}", cfgs);

    // Validate the configuration.
    match cfgs.validate() {
        CameraConfigurationStatus::Valid => println!("Camera configuration is valid!"),
        CameraConfigurationStatus::Adjusted => {
            println!("Camera configuration was adjusted: {:#?}", cfgs)
        }
        CameraConfigurationStatus::Invalid => panic!("Error validating camera configuration"),
    }

    // Apply the configuration to the camera.
    cam.configure(&mut cfgs)
        .expect("Unable to configure camera");

    // Allocate frame buffers for the stream.
    let mut alloc = FrameBufferAllocator::new(&cam);
    let cfg = cfgs.get(0).unwrap();
    let stream = cfg.stream().unwrap();
    let buffers = alloc
        .alloc(&stream)
        .expect("Failed to allocate frame buffers");
    println!("Allocated {} buffers", buffers.len());

    // Map each buffer into memory so that we can access the raw data.
    let mapped_buffers: Vec<_> = buffers
        .into_iter()
        .map(|buf| MemoryMappedFrameBuffer::new(buf).expect("Failed to map framebuffer"))
        .collect();

    // Create capture requests and attach each mapped buffer.
    let requests: Vec<_> = mapped_buffers
        .into_iter()
        .enumerate()
        .map(|(i, buf)| {
            let mut req = cam
                .create_request(Some(i as u64))
                .expect("Failed to create request");
            req.add_buffer(&stream, buf)
                .expect("Failed to attach buffer to request");
            req
        })
        .collect();

    // Set up a channel to receive completed capture requests.
    let (tx, rx) = std::sync::mpsc::channel();
    cam.on_request_completed(move |req| {
        tx.send(req).expect("Failed to send completed request");
    });

    // Start the camera.
    cam.start(None).expect("Unable to start camera");

    // Enqueue all the requests for the camera to begin capturing.
    for req in requests {
        println!("Queuing request: {:#?}", req);
        cam.queue_request(req).expect("Failed to queue request");
    }

    // Open (or create) the output file in append mode.
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&filename)
        .expect("Unable to create output file");

    // Capture a set number of frames (here 60 frames, adjust as needed).
    for frame_index in 0..60 {
        println!("Waiting for frame {}", frame_index);
        let mut req = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("Timeout waiting for frame");
        // println!(
        //     "Frame {} captured, metadata: {:#?}",
        //     frame_index,
        //     req.metadata()
        // );

        // Retrieve the framebuffer for our stream.
        let framebuffer: &MemoryMappedFrameBuffer<FrameBuffer> = req.buffer(&stream).unwrap();

        // Since we mapped the buffer, we can access its data.
        let fdata = framebuffer.data();
        let frame_data = fdata.get(0).expect("No data plane available");
        // The actual data length is given by the metadata.
        let bytes_used = framebuffer
            .metadata()
            .unwrap()
            .planes()
            .get(0)
            .unwrap()
            .bytes_used as usize;

        println!("YAYYAYYAYAY: {:?}", fdata.len());

        let current_brightness = {
            let mut sum: u64 = 0;
            let mut pixel_count = 0;

            // Sum up all pixel values (RGB)
            for ((&r, &g), &b) in fdata.get(0).unwrap().iter().zip(fdata.get(1).unwrap().iter()).zip(fdata.get(2).unwrap().iter()) {
                // Average the RGB channels for each pixel
                sum += (r as u64 + g as u64 + b as u64) / 3;
                pixel_count += 1;
            }

            if pixel_count == 0 {
                0.0
            } else {
                // Calculate average brightness (0-255)
                (sum as f64) / (pixel_count as f64)
            }
        };

        inserter.insert_float64(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().try_into().unwrap(), current_brightness);

        // Write the valid frame data to the output file.
        file.write_all(&frame_data[..bytes_used])
            .expect("Failed to write frame data to file");
        println!(
            "Wrote {} bytes for frame {} to {}",
            bytes_used, frame_index, filename
        );

        // Recycle the request to be reused for capturing the next frame.
        req.reuse(ReuseFlag::REUSE_BUFFERS);
        cam.queue_request(req).expect("Failed to requeue request");
    }

    println!("Video capture complete. Output saved to {}", filename);

    Ok(())
}
