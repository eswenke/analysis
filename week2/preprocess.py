import polars as pl
import gzip
import time
import shutil


def unzip(file_path): 
    with gzip.open(file_path, 'rb') as f_in:
        with open('2022_place_canvas_history.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def preprocess_data_lazy(file_path): 
    with open(file_path, mode='rt', encoding='utf-8') as f:
        df = pl.scan_csv(f)
        lazy_processed = (
            df
            # parse the datetime column into a proper datetime type
            .with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f %Z").dt.truncate("1h"), #.dt.strftime("%Y-%m-%d %H"),
                pl.col("user_id").cast(pl.Categorical).to_physical()
            )
            .collect()
        )
        
        lazy_processed.write_csv("2022_place_canvas_history_truncated.csv")
        
        # collect the lazy frame into a dataframe and write to parquet
        # processed_df = lazy_processed.collect()
        # processed_df.write_parquet("2022_place_canvas_history.parquet")

        # we need to:
        #   1. add an hour column by extracting the hour from the timestamp column
        #   2. group by hour, pixel_color, and coordinates
        #   3. aggregate the count of each pixel_color and coordinates
        # df_grouped = (
        #     lazy_processed
        #     .group_by(["timestamp", "pixel_color", "coordinate"])  # Adjust the column names as needed
        #     .agg([
        #         pl.count("pixel_color").alias("pixel_count"),
        #         pl.count("coordinate").alias("coordinate_count")
        #     ])
        # ).collect()

        # write the grouped dataframe to a CSV file
        # df_grouped.write_csv("2022_place_canvas_history_preprocessed_grouped.csv")

        # write the dataframe to a parquet file
        # df_grouped.write_parquet("2022_place_canvas_history.parquet")

        return
    
def preprocess_data_lazy(file_path): 
    with open(file_path, mode='rt', encoding='utf-8') as f:
        df = pl.read_csv(f)
    
def main():
    # get start time
    start_time = time.perf_counter_ns()

    # unzip
    # unzip("../week1/2022_place_canvas_history.csv.gzip")

    # preprocess data
    preprocess_data_lazy("2022_place_canvas_history.csv")
    preprocess_data_eager("2022_place_canvas_history.csv")

    # get end_time
    end_time = time.perf_counter_ns()

    print(f"Execution time: {((end_time - start_time)//1000000)} ms")

    return 0

if __name__ == '__main__':
    main()