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

def preprocess_data(file_path):
    lazy_rplace = pl.scan_csv(file_path)
    # we also need to:
    #   1. add an hour column by extracting the hour from the timestamp column
    #   2. group by hour, pixel_color, and coordinates
    #   3. aggregate the count of each pixel_color and coordinates


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
    start_hour, end_hour = validate_input(start_date, start_hour, end_date, end_hour)
    
    # preprocess data?
    preprocessed_data = preprocess_data("../week1/2022_place_canvas_history.csv.gzip")
    
    # get end_time
    end_time = time.perf_counter_ns()

    return 0

if __name__ == '__main__':
    main()