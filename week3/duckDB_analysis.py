import duckdb as ddb
import sys
import webcolors as wc
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

def get_color_ranks(start, end):
    # report a ranking of colors during specified time range
    print("getting color ranks...")
    
    result = ddb.sql(f"""
        SELECT pixel_color, COUNT(*) AS count
        FROM filtered
        GROUP BY pixel_color
        ORDER BY count DESC
    """).fetchall()
    
    print(result)
    print()
    
    return

def get_avg_session(start, end):
    # define a session as a user’s activity within a 15-minute window of inactivity. 
    # return the average session length in seconds during the specified timeframe. 
    # only include cases where a user had more than one pixel placement during the time period in the average.
    print("getting average session...")
    
    # result = ddb.sql(f"""

    # """).fetchall()
    
    # print(result)
    print()

    return

def get_pixel_percentiles(start, end):
    # calculate the 50th, 75th, 90th, and 99th percentiles 
    # of the number of pixels placed by users during the specified timeframe.
    print("getting pixel percentiles...")
    
    # result = ddb.sql(f"""

    # """).fetchall()
    
    # print(result)
    print()
    
    return

def get_first_time_users(start, end):
    # count how many users placed their first pixel ever within the specified timeframe
    print("getting first time users...")
    
    # result = ddb.sql(f"""

    # """).fetchall()
    
    # print(result)
    print()
    
    return

def get_analysis(file_path, start, end):
    # call the get_* functions within here to preserve the ddb data scope
    # might need to pass data into each function to get this to work
    data = ddb.read_parquet(file_path)
    
    filtered = ddb.sql(f"""
        SELECT *
        FROM data
        WHERE timestamp >= '{start}' AND timestamp <= '{end}'
    """).create_view("filtered")
    
    get_color_ranks(start, end)
    get_avg_session(start, end)
    get_pixel_percentiles(start, end)
    get_first_time_users(start, end)
    
    return


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
    get_analysis("../week2/2022pyarrow.parquet", start, end)
        
    # get end_time
    end_time = time.perf_counter_ns()
    
    print(f"time range: {start} to {end}")
    print(f"execution time: {((end_time - start_time)//1000000)} ms")
    
if __name__ == '__main__':
    main()