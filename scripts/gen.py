import csv
import random
import argparse

def generate_time_series_csv(filename, num_rows, start_time, dtype):       
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp", "value"])
        
        prev_timestamp = start_time
        for i in range(num_rows):
            # make next timestamp
            random_delta = 0 if i == 0 else round(random.uniform(1, 200))
            timestamp = prev_timestamp + random_delta
            prev_timestamp = timestamp

            # make next value
            if dtype == "uint":
                value = round(random.uniform(0, 200))
            elif dtype == "int":
                value = round(random.uniform(-100, 100))
            elif dtype == "float":
                value = round(random.uniform(-100, 100), 2)
            else:
                print("--dtype invalid.")
                print("Usage: uint | int | float")
            
            # write value
            writer.writerow([timestamp, value])
    
    print(f"CSV file '{filename}' generated successfully with {num_rows} rows.")

def main():
    parser = argparse.ArgumentParser(description="Generate random time series data in CSV format.")
    parser.add_argument("--filename", type=str, default="time_series.csv", help="Output CSV filename.")
    parser.add_argument("--num-rows", type=int, default=100, help="Number of rows to generate.")
    parser.add_argument("--start-time", type=int, default=0, help="Start timestamp.")
    parser.add_argument("--dtype", type=str, default="uint", help="Data type for CSV - uint | int | float")
    
    args = parser.parse_args()
    generate_time_series_csv(args.filename, args.num_rows, args.start_time, args.dtype)

if __name__ == "__main__":
    main()
