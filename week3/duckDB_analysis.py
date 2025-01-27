import sys
import time
import duckdb as ddb
from colory.color import Color
from datetime import datetime

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

    for i, (color, count) in enumerate(result):
        try:
            c = Color(color, 'xkcd')
            name = c.name
        except ValueError:
            name = "Unknown"
        print(f"{name.ljust(20)} {str(count).rjust(7)}")

    print()
    
    return

def get_avg_session(start, end):
    # define a session as a user’s activity within a 15-minute window of inactivity. 
    # return the average session length in seconds during the specified timeframe. 
    # only include cases where a user had more than one pixel placement during the time period in the average
    # so a session is active as long as a user has placed a pixel within the 15 min window
    # if 15 minutes passes and a pixel hasn't been placed in that time, the session is ended
    print("getting average session...")
    
    # create a cte: for every user placement, add a lagged timestamp
    # then go through that cte and calculate each user's average session length (case statement for each comparison)
    # if i have previous session length that is less than 15 minutes, i add the current session length to it, and
    # if 
    # gather the results from that cte and take the total average.
    result = ddb.sql(f"""
        WITH lagged_timestamps AS (
            SELECT 
                user_id,
                LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS session_length
            FROM filtered
        ),
        averages AS (
            SELECT 
                user_id,
                AVG(session_length) AS avg_session_length
            FROM lagged_timestamps
            GROUP BY user_id
        )

    """).fetchall()
    
    print(result)
    print()

    return

def get_pixel_percentiles(start, end):
    # calculate the 50th, 75th, 90th, and 99th percentiles 
    # of the number of pixels placed by users during the specified timeframe.
    print("getting pixel percentiles...")
    
    result = ddb.sql(f"""
        SELECT 
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pixel_count ASC) AS percentile_50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pixel_count ASC) AS percentile_75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pixel_count ASC) AS percentile_90,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pixel_count ASC) AS percentile_99
        FROM (
            SELECT user_id, COUNT(pixel_color) as pixel_count
            FROM filtered
            GROUP BY user_id
        ) AS user_pixel_counts;
    """).fetchall()
    
    print(f"50th percentile: {result[0][0]}")
    print(f"75th percentile: {result[0][1]}")
    print(f"90th percentile: {result[0][2]}")
    print(f"99th percentile: {result[0][3]}")

    print()
    
    return

def get_first_time_users(start, end, data):
    # count how many users placed their first pixel ever within the specified timeframe
    print("getting first time users...")

    result = ddb.sql(f"""
        WITH first_placements AS (
            SELECT user_id, MIN(timestamp) as timestamp
            FROM data
            GROUP BY user_id
        )
        SELECT COUNT(*) as count
        FROM first_placements
        WHERE timestamp >= '{start}' AND timestamp <= '{end}'
    """).fetchall()
    
    print(result[0][0])
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
    get_first_time_users(start, end, data)
    
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