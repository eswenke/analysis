import polars as pl
import gzip
import time
import shutil


def unzip(file_path): 
    with gzip.open(file_path, 'rb') as f_in:
        with open('2022_place_canvas_history.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def preprocess_data_lazy(file_path):
    # this approach didn't work. try:
    #   1. polars read_batched_csv or something and write to parquet
    #   2. pandas hard read of the csv and write to parquet
    #   3. duckdb read of csv and write to parquet
    #   ****** make sure to at least convert timestamp to datetime in some way (might as well compress user_id too)
    
    # df = pl.scan_csv(file_path)

    # print("started preprocessing")
    
    # # Use an expression to parse timestamps based on multiple patterns
    # lazy_processed = (
    #     df
    #     .with_columns(
    #         # Try parsing with microseconds (%f), fallback to parsing without it
    #         pl.when(pl.col("timestamp").str.contains(r"\.\d{1,6}"))
    #         .then(pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f"))
    #         .otherwise(pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"))
    #         .alias("parsed_timestamp")
    #     )
    #     # Truncate the datetime to the hour level
    #     .with_columns(pl.col("parsed_timestamp").dt.truncate("1h"))
    #     # Cast the user_id to categorical
    #     .with_columns(pl.col("user_id").cast(pl.Categorical).to_physical())
    # ).collect()
    
    # # Save the processed data as a Parquet file
    # lazy_processed.write_parquet("2022_place_canvas_history.parquet")

        
def main():
    # get start time
    start_time = time.perf_counter_ns()

    # # unzip
    # unzip("2022_place_canvas_history.csv.gzip")

    # preprocess data
    preprocess_data_lazy("2022_place_canvas_history.csv")

    # get end_time
    end_time = time.perf_counter_ns()

    print(f"Execution time: {((end_time - start_time)//1000000)} ms")

    return 0

if __name__ == '__main__':
    main()