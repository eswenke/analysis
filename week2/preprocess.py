import polars as pl
import numpy as np
import sys
import gzip
import csv
from datetime import datetime
import time


def preprocess_data(file_path):
    # read the CSV file lazily
    with gzip.open(file_path, mode='rt', encoding='utf-8') as file:
        df = pl.scan_csv(file.read())
        lazy_processed = (
            df
            # parse the datetime column into a proper datetime type
            .with_columns(pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f %Z"))
        )
        print(lazy_processed.head(10).collect())    

    return
    

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