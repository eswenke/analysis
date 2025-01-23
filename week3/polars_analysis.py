import polars as pl
import numpy as np
import sys
import gzip
import csv
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
    lazy_rplace = pl.scan_parquet(file_path)

    # filter by timestamp
    filtered_data = lazy_rplace.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))

    # count occurrences of pixel_color and get max
    pixel_color_count = (
        filtered_data
        .group_by("pixel_color")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    ).collect()

    max_pixel_color = pixel_color_count[0, "pixel_color"]
    max_pixel_color_count = pixel_color_count[0, "count"]

    # count occurrences of coordinate and get max
    coordinate_count = (
        filtered_data
        .group_by("coordinate")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    ).collect()

    max_coordinate = coordinate_count[0, "coordinate"]
    max_coordinate_count = coordinate_count[0, "count"]
    
    return max_pixel_color, max_coordinate
    

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
    color, coord = get_counts("../week2/2022pyarrow.parquet", start, end)
    
    # get end_time
    end_time = time.perf_counter_ns()

    print(f"time range: {start} to {end}")
    print(f"execution time: {((end_time - start_time)//1000000)} ms")
    print(f"most placed color: {color}")
    print(f"most placed coordinate: {coord}")

    return 0

if __name__ == '__main__':
    main()