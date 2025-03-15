import cv2
from picamera import PiCamera
from picamera.array import PiRGBArray

CAMERA_RESOLUTION = (640, 480)


def capture_video():
    # Press 'q' to stop the program.
    try:
        # Setup
        camera = PiCamera()
        camera.resolution = CAMERA_RESOLUTION
        output = PiRGBArray(camera)
        # Update and draw
        is_running = True
        while is_running:
            camera.capture(output, "bgr", use_video_port=True)
            image = output.array
            output.truncate(0)
            # Display on screen
            cv2.imshow("frame", image)
            # Exit the loop on event
            key = cv2.waitKey(10) & 0xFF
            if key == ord("q"):
                is_running = False
    finally:
        # Release resources
        output.close()
        camera.close()


def capture_still():
    try:
        # Setup
        camera = PiCamera()
        output = PiRGBArray(camera)
        # Capture
        camera.capture(output, format="bgr", use_video_port=False)
        frame = output.array
        # Save the image to disk.
        cv2.imwrite("frame.jpg", frame)
    finally:
        # Release resources
        output.close()
        camera.close()


if __name__ == "__main__":
    # capture_still()
    capture_video()
