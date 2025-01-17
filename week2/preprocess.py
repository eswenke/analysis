import polars as pl
import numpy as np
import sys
import gzip
import csv
from datetime import datetime
import time


def preprocess_data(file_path):
    lazy_rplace = pl.scan_csv(file_path, infer_schema_length=10000, low_memory=True)
    print(lazy_rplace.schema)
    
    return 0

    # we need to:
    #   1. add an hour column by extracting the hour from the timestamp column
    #   2. group by hour, pixel_color, and coordinates
    #   3. aggregate the count of each pixel_color and coordinates

    # add hour column
    lazy_rplace = lazy_rplace.with_columns(
        (pl.col('timestamp').dt.hour()).alias('hour')
    )

    # group by hour, pixel_color, and coordinates and aggregate
    aggregated_data = lazy_rplace.group_by(['hour', 'pixel_color', 'coordinates']).agg(
        count_pixel_color=('pixel_color', 'count'),
        count_coordinates=('coordinates', 'count')
    )

    print(aggregated_data.head(10))

    # aggregated_data.write_csv('aggregated_data.csv')

    return aggregated_data
    

def main():
    # get start time
    start_time = time.perf_counter_ns()

    # preprocess data?
    preprocess_data("../week1/2022_place_canvas_history.csv.gzip")
    
    # get end_time
    end_time = time.perf_counter_ns()

    print(f"Execution time: {((end_time - start_time)//1000000)} ms")

    return 0

if __name__ == '__main__':
    main()