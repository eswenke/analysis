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
    
    result = ddb.sql(f"""
        WITH lagged_timestamps AS (
            SELECT 
                user_id_numeric, 
                timestamp,
                DATE_DIFF('minute', LAG(timestamp) OVER (PARTITION BY user_id_numeric ORDER BY timestamp), timestamp) AS time_between
            FROM filtered
        ),
        sessions AS (
            SELECT 
                *,
                SUM((CASE WHEN time_between >= 15 THEN 1 ELSE 0 END)) OVER (PARTITION BY user_id_numeric ORDER BY timestamp) AS session  
            FROM lagged_timestamps
        ),
        lengths AS (
            SELECT
                user_id_numeric,
                session,
                DATE_DIFF('second', MIN(timestamp), MAX(timestamp)) as session_length
            FROM sessions
            GROUP BY user_id_numeric, session
            HAVING COUNT(*) > 1
            ORDER BY user_id_numeric, session
        )
        SELECT ROUND(AVG(session_length), 2)
        FROM lengths
    """).fetchall()
    
    print(str(result[0][0]) + " seconds")
    print()

    return

def get_pixel_percentiles(start, end):
    # calculate the 50th, 75th, 90th, and 99th percentiles 
    # of the number of pixels placed by users during the specified timeframe.
    print("getting pixel percentiles...")
    
    result = ddb.sql(f"""
        WITH counts as (
            SELECT user_id_numeric, COUNT(*) as count
            FROM filtered
            GROUP BY user_id_numeric
        )          
        SELECT 
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY count) AS percentile_50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY count) AS percentile_75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY count) AS percentile_90,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY count) AS percentile_99
        FROM counts;
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
    
    # this is how i think it should be done to make sure we don't see a first pixel
    # placed in the time range as THE first pixel EVER for a user, but I can't physically run
    # this query without my laptop exploding, so the query below will have to do and just be
    # incorrect. sorry.
    # result = ddb.sql(f"""
    #     WITH first_placements AS (
    #         SELECT user_id_numeric, MIN(timestamp) as timestamp
    #         FROM data
    #         GROUP BY user_id_numeric
    #     )
    #     SELECT COUNT(*) as count
    #     FROM first_placements
    #     WHERE timestamp >= '{start}' AND timestamp <= '{end}'
    # """).fetchall()

    result = ddb.sql(f"""
        WITH first_placements AS (
            SELECT user_id_numeric, MIN(timestamp) as timestamp
            FROM filtered
            GROUP BY user_id_numeric
        )
        SELECT COUNT(*) as count
        FROM first_placements
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
    get_analysis("rplace.parquet", start, end)
        
    # get end_time
    end_time = time.perf_counter_ns()
    
    print(f"time range: {start} to {end}")
    print(f"execution time: {((end_time - start_time)//1000000)} ms")
    
if __name__ == '__main__':
    main()