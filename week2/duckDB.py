import duckdb as ddb
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

    start_time = datetime.now()  # recording the start time
    data = ddb.read_csv("2022_place_canvas_history.csv")
    print(ddb.sql("select count(*) from data"))
    end_time = datetime.now()  # recording the end time
    execution_time = (end_time - start_time).total_seconds()
    print(f"Execution time: {execution_time} seconds")

    return 0, 0

    # connect
    con = ddb.connect(":default:")

    # read the CSV file into DuckDB
    con.execute(f"""
        CREATE TABLE canvas_data AS
        SELECT * FROM read_csv_auto('{file_path}')
    """)

    # query to find the most placed pixel color and coordinate within the time range
    result = con.execute(f"""
        SELECT pixel_color, coordinate, COUNT(*) AS count
        FROM canvas_data
        WHERE timestamp >= '{start}' AND timestamp <= '{end}'
        GROUP BY pixel_color, coordinate
        ORDER BY count DESC
        LIMIT 1
    """).fetchone()  # fetch top result

    con.close()  # close the connection
    return result  


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
    color, coord = get_counts("2022_place_canvas_history.csv", start, end)
    
    # get end_time
    end_time = time.perf_counter_ns()

    print(f"time range: {start} to {end}")
    print(f"execution time: {((end_time - start_time)//1000000)} ms")
    print(f"most placed color: {color}")
    print(f"most placed coordinate: {coord}")
    
if __name__ == '__main__':
    main()