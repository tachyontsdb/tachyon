# Raspberry Pi Camera Brightness Monitor

This Rust application monitors the brightness level of a Raspberry Pi camera feed and logs the values to the console. It also detects significant changes in brightness.

## Prerequisites

- Raspberry Pi with camera module connected
- Rust installed on the Raspberry Pi

## Installation

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
1. Initialize the camera
2. Start capturing frames at regular intervals (every 500ms)
3. Calculate and display the current brightness level
4. Alert when significant brightness changes are detected

## Configuration

You can adjust the following parameters in the code:
- `brightness_threshold`: Controls how significant a brightness change needs to be before it's reported (default: 10.0)
- Capture interval: Change the sleep duration to adjust how frequently frames are captured (default: 500ms)

## How It Works

The application calculates the average brightness by taking the mean value of all pixels in each frame. The brightness scale is from 0 (completely dark) to 255 (maximum brightness).
