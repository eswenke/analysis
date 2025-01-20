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
    lazy_rplace = pl.scan_csv(file_path, low_memory=True)
                              # try_parse_dates=True)
                              # schema={"timestamp": pl.Datetime, "user_id": pl.String, "pixel_color": pl.String, "coordinate": pl.String})

    # filter by timestamp and group by pixel_color and coordinate
    result = (
        lazy_rplace
        .filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))
        .group_by(["pixel_color", "coordinate"])
        .agg(pl.len().alias("count")) 
        .sort("count", descending=True)
        .select(["pixel_color", "coordinate", "count"])  
        .collect()  
    )

    most_placed_color = result[0, "pixel_color"]
    most_placed_coordinate = result[0, "coordinate"]
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
    
    # preprocess data?
    color, coord = get_counts("2022_place_canvas_history.csv", start, end)
    
    # get end_time
    end_time = time.perf_counter_ns()

    print(f"time range: {start} to {end}")
    print(f"execution time: {((end_time - start_time)//1000000)} ms")
    print(f"most placed color: {color}")
    print(f"most placed coordinate: {coord}")

    return 0

if __name__ == '__main__':
    main()