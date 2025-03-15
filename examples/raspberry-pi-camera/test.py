import cv2

def main():
    # Open the camera (0 for the default camera, change if using a USB camera)
    cap = cv2.VideoCapture(0)
    
    # Set resolution (optional)
    cap.set(3, 640)  # Width
    cap.set(4, 480)  # Height
    
    if not cap.isOpened():
        print("Error: Could not open camera.")
        return
    
    while True:
        ret, frame = cap.read()
        if not ret:
            print("Error: Failed to capture image.")
            break
        
        # Display the frame
        cv2.imshow("Camera Stream", frame)
        
        # Press 'q' to exit
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    
    # Release resources
    cap.release()
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()
