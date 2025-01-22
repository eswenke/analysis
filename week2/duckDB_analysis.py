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
    data = ddb.read_csv(file_path)
    
    # shortest way to do this (implicitly converts timestamps)
    # result = ddb.sql(f"""
    #     SELECT pixel_color, coordinate, COUNT(*) AS count
    #     FROM data
    #     WHERE timestamp >= '{start}' AND timestamp <= '{end}'
    #     GROUP BY pixel_color, coordinate
    #     ORDER BY count DESC
    #     LIMIT 1
    # """).fetchone() 
    
    # this does the same thing, just more complicated?
    result = ddb.sql(f"""
            SELECT
            pixel_color,
            MAX(color_count) AS color_count,
            coordinate,
            MAX(coord_count) AS coord_count
            FROM (
                SELECT
                    pixel_color,
                    COUNT(pixel_color) AS color_count,
                    coordinate,
                    COUNT(coordinate) AS coord_count
                FROM data
                WHERE
                    CAST(timestamp AS TIMESTAMP) >= '{start}' AND
                    CAST(timestamp AS TIMESTAMP) <= '{end}'
                GROUP BY
                    pixel_color, coordinate
            )
            GROUP BY
                pixel_color, coordinate
            ORDER BY
                color_count DESC, coord_count DESC
            LIMIT 1
            """).fetchall()
    
    print(result)
        
    return result[0][1], result[0][2]


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