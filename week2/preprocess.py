import polars as pl
import numpy as np
import sys
import gzip
import csv
from datetime import datetime
import time


def preprocess_data(file_path):
    with gzip.open(file_path, mode='rt', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # converting the file to dictionary
        # by first converting to list
        # and then converting the list to dict
        dict_from_csv = dict(list(reader)[0])
    
        # making a list from the keys of the dict
        list_of_column_names = list(dict_from_csv.keys())
    
        # displaying the list of column names
        print("List of column names : ", list_of_column_names)

    # lazy_rplace = pl.scan_csv(file_path)
    

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