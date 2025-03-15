# Raspberry Pi Camera Brightness Monitor

This Rust application monitors the brightness level of a Raspberry Pi camera feed using libcamera and logs the values to the console. It also detects significant changes in brightness. This implementation is compatible with Raspberry Pi 4B and newer models.

## Prerequisites

- Raspberry Pi 4B or newer with camera module connected
- Rust installed on the Raspberry Pi
- libcamera and development libraries installed

## Installation

### Install libcamera dependencies

Before building the application, you need to install the libcamera development libraries:

```bash
sudo apt update
sudo apt install -y libcamera-dev libcamera-apps-lite
```

### Build the application

1. Clone this repository
2. Navigate to the project directory
3. Build the application:

```bash
cargo build --release
```

## Usage

Run the application:

```bash
cargo run --release
```

The application will:
1. Initialize the camera using libcamera
2. Open a camera stream
3. Start capturing frames at regular intervals (every 500ms)
4. Calculate and display the current brightness level
5. Alert when significant brightness changes are detected

## Configuration

You can adjust the following parameters in the code:
- `brightness_threshold`: Controls how significant a brightness change needs to be before it's reported (default: 10.0)
- Capture interval: Change the sleep duration to adjust how frequently frames are captured (default: 500ms)
- Resolution: Modify the `width` and `height` variables to change the camera resolution (default: 640x480)

## How It Works

The application uses the nokhwa library with libcamera backend to access the Raspberry Pi camera. For each frame:

1. It converts the camera frame to an RGB image
2. Calculates the average brightness by averaging all pixel values
3. Compares the current brightness to the previous frame
4. Reports significant changes in brightness

The brightness scale is from 0 (completely dark) to 255 (maximum brightness).
