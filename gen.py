import csv
import random
import argparse

def generate_time_series_csv(filename, num_rows, start_time, interval_seconds):            
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp", "value"])
        
        for i in range(num_rows):
            timestamp = start_time + i * interval_seconds
            value = round(random.uniform(0, 100), 2)  # Random float between 0 and 100
            writer.writerow([timestamp, value])
    
    print(f"CSV file '{filename}' generated successfully with {num_rows} rows.")

def main():
    parser = argparse.ArgumentParser(description="Generate random time series data in CSV format.")
    parser.add_argument("--filename", type=str, default="time_series.csv", help="Output CSV filename.")
    parser.add_argument("--num-rows", type=int, default=100, help="Number of rows to generate.")
    parser.add_argument("--start-time", type=int, default=0, help="Start timestamp.")
    parser.add_argument("--interval-time", type=int, default=10, help="Interval between timestamps.")
    
    args = parser.parse_args()
    generate_time_series_csv(args.filename, args.num_rows, args.start_time, args.interval_time)

if __name__ == "__main__":
    main()
