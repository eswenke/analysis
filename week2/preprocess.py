import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gzip
import time
import shutil


def unzip(file_path): 
    with gzip.open(file_path, 'rb') as f_in:
        with open('2022_place_canvas_history.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def process_chunk(chunk):
    # Initialize a list to store parsed timestamps
    parsed_timestamps = []

    for timestamp in chunk['timestamp']:
        try:
            # Attempt to parse with microseconds
            parsed_time = pd.to_datetime(timestamp, format='%Y-%m-%d %H:%M:%S.%f', errors='raise')
        except ValueError:
            try:
                # Attempt to parse without microseconds
                parsed_time = pd.to_datetime(timestamp, format='%Y-%m-%d %H:%M:%S', errors='raise')
            except ValueError:
                # If both attempts fail, keep the original string
                parsed_time = timestamp  # or you can choose to set it to None or another placeholder

        parsed_timestamps.append(parsed_time)

    # Assign the parsed timestamps back to the DataFrame
    chunk['timestamp'] = parsed_timestamps
    
    # Convert 'user_id' column to unique integers
    chunk['user_id'] = chunk['user_id'].astype('category').cat.codes
    
    return chunk

def preprocess_data_lazy(file_path): 
    chunk_size = 1000000
    writer = None
    
    try:
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            # normalize timestamp column
            chunk = process_chunk(chunk)

            #convert chunk to arrow table
            table = pa.Table.from_pandas(chunk)

            if write is None:
                write = pq.ParquetWriter(
                    "2022_place_canvas_history.parquet", table.schema, compression='snappy'
                )

            write.write_table(table)
            print(f"processed chunk {i+1}")

    finally:
        if writer:
            write.close()
    
    return

        
def main():
    # get start time
    start_time = time.perf_counter_ns()

    # unzip
    # unzip("../week1/2022_place_canvas_history.csv.gzip")

    # preprocess data
    preprocess_data_lazy("2022_place_canvas_history.csv")
    # preprocess_data_eager("2022_place_canvas_history.csv")

    # get end_time
    end_time = time.perf_counter_ns()

    print(f"Execution time: {((end_time - start_time)//1000000)} ms")

    return 0

if __name__ == '__main__':
    main()