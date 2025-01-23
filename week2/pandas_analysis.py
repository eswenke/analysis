import pandas as pd
import sys
from datetime import datetime
import time

# check input is valid
def validate_input(start_date, start_hour, end_date, end_hour):
    start = datetime.strptime(f"{start_date} {start_hour}", "%Y-%m-%d %H")
    end = datetime.strptime(f"{end_date} {end_hour}", "%Y-%m-%d %H")

    # check start date is before end date
    if end <= start:
        raise ValueError("start hour must be before end hour")
    return start, end

def get_counts(file_path, start, end):
    df = pd.read_parquet(file_path, columns=['timestamp', 'pixel_color', 'coordinate'])
    print("after read parquet")

    # filter based on time range
    filtered_data = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
    print("after timestamp")

    # get most common pixel_color and coordinate
    most_placed_color = filtered_data.groupby['pixel_color'].size().idxmax()
    most_placed_coordinate = filtered_data.groupby['coordinate'].size().idxmax()
    print("after groupbys")

    return most_placed_color, most_placed_coordinate


def get_counts_in_batches(file_path, start, end):
    chunk_size=1_000_000

    # Read the data in chunks
    counts_color = {}
    counts_coordinate = {}
    for chunk in pd.read_parquet(file_path, chunksize=chunk_size):
        # Filter based on time range
        chunk = chunk[(chunk['timestamp'] >= start) & (chunk['timestamp'] <= end)]
        
        # Count occurrences of pixel_color and coordinate in this chunk
        chunk_color_counts = chunk['pixel_color'].value_counts().to_dict()
        chunk_coordinate_counts = chunk['coordinate'].value_counts().to_dict()

        # Update global counts
        for k, v in chunk_color_counts.items():
            counts_color[k] = counts_color.get(k, 0) + v
        for k, v in chunk_coordinate_counts.items():
            counts_coordinate[k] = counts_coordinate.get(k, 0) + v

    # Determine the most placed color and coordinate
    most_placed_color = max(counts_color, key=counts_color.get)
    most_placed_coordinate = max(counts_coordinate, key=counts_coordinate.get)

    return most_placed_color, most_placed_coordinate


def main():
    # start time
    start_time = time.perf_counter_ns()

    # validate command line args count
    if len(sys.argv) != 5:
        print("Usage: python3 rplace.py <start_date> <start_hour> <end_date> <end_hour>")
        sys.exit()
    
    # get command line args
    start_date = sys.argv[1]
    start_hour = sys.argv[2]
    end_date = sys.argv[3]
    end_hour = sys.argv[4]

    # validate input and read in data
    start, end = validate_input(start_date, start_hour, end_date, end_hour)
    
    # get color and coord max for given time range
    color, coord = get_counts("2022pyarrow.parquet", start, end)
    # color, coord = get_counts_in_batches("2022pyarrow.parquet", start, end)
    
    # get end_time
    end_time = time.perf_counter_ns()

    print(f"time range: {start} to {end}")
    print(f"execution time: {((end_time - start_time)//1000000)} ms")
    print(f"most placed color: {color}")
    print(f"most placed coordinate: {coord}")

    
if __name__ == '__main__':
    main()