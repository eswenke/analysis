import pandas as pd
import sys
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
    df = pd.read_csv(file_path, usecols=["timestamp", "pixel_color", "coordinate"])
    print(f"num rows in df: {df.shape[0]}")
    
    # try to read in chunks
    # chunksize = 10 ** 6
    # for chunk in pd.read_csv(file_path, chunksize=chunksize):
    #     for index, row in chunk.iterrows():
    #         print(row)

    # # crashes here
    # df = pd.read_csv(file_path)
    
    # # convert the 'timestamp' column to datetime
    # df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%d %H:%M:%S%.f %Z")
    
    # # filter based on newly converted datetime timestamp
    # filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
    
    # # group by 'pixel_color' and 'coordinate' and count
    # counts = filtered_df.groupby(['pixel_color', 'coordinate']).size().reset_index(name='count')

    # # sort by count in descending order
    # max_counts = counts.sort_values(by='count', ascending=False).head(1)

    # return max_counts['pixel_color'].values[0], max_counts['coordinate'].values[0]
    
    return 0, 0


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