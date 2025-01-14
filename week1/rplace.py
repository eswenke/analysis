import sys
import gzip
import csv
from datetime import datetime
import time

# read in r/place data
def read_data(file_path, start_hour, end_hour):
    data = []
    with gzip.open(file_path, mode='rt', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # normalize the timestamp to ensure it has fractional seconds
            timestamp_str = row['timestamp']
            if ' UTC' in timestamp_str:
                timestamp_str = timestamp_str.replace(' UTC', '')

            # check if the timestamp doesnt contain a decimal point (handles value error)
            if '.' not in timestamp_str:
                timestamp_str += '.000'

            row_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")

            # check that row is within our timeframe provided
            if start_hour <= row_time <= end_hour:
                data.append(row)
    return data

# check input is valid
def validate_input(start_date, start_hour, end_date, end_hour):
    start = datetime.strptime(f"{start_date} {start_hour}", "%Y-%m-%d %H")
    end = datetime.strptime(f"{end_date} {end_hour}", "%Y-%m-%d %H")

    # check start date is before end date

    if end <= start:
        raise ValueError("start hour must be before end hour")
    return start, end

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
    data = read_data('2022_place_canvas_history.csv.gzip', start_hour, end_hour)

    colors = {}
    pixels = {}

    # color count
    for row in data:
        color = row['pixel_color']
        if color in colors:
            colors[color] += 1
        else:
            colors[color] = 1

    color = max(colors, key=colors.get)

    ## pixel count
    for row in data:
        pixel = row['coordinate']
        if pixel in pixels:
            pixels[pixel] += 1
        else:
            pixels[pixel] = 1

    pixel = max(pixels, key=pixels.get)

    # end time
    end_time = time.perf_counter_ns()

    # print results
    print(f"Time Frame: {start_hour} to {end_hour}")
    print(f"Execution time: {((end_time - start_time)//1000000)} ms")
    print(f"Most placed color: {color}")
    print(f"Most placed pixel location: {pixel}")

if __name__ == '__main__':
    main()