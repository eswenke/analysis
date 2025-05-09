import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl

csv_file = "all_reviews/all_reviews.csv"
parquet_file = "all_reviews/all_reviews.parquet"

BLOCK_SIZE = 10_000_000

# define expected column types (force all columns to be strings if unsure)
read_options = pv.ReadOptions(block_size=BLOCK_SIZE)
convert_options = pv.ConvertOptions(column_types={"steam_china_location": pa.string()})  

csv_reader = pv.open_csv(csv_file, read_options=read_options, convert_options=convert_options)
parquet_writer = None

try:
    for record_batch in csv_reader:
        print(f"Processing batch with {record_batch.num_rows} rows...")

        df = pl.from_arrow(record_batch)

        # columns not needed
        df = df.drop(["steam_china_location", "hidden_in_steam_china", "recommendationid", "appid"])

        # largely shrinking from 64 to 32 where possible right now
        df = df.with_columns(
            pl.col("weighted_vote_score").cast(pl.Float32).alias("weighted_vote_score"),
            pl.col("author_num_games_owned").cast(pl.Int32).alias("author_num_games_owned"),
            pl.col("author_num_reviews").cast(pl.Int32).alias("author_num_reviews"),
            pl.col("author_playtime_forever").cast(pl.Int32).alias("author_playtime_forever"),
            pl.col("author_playtime_last_two_weeks").cast(pl.Int32).alias("author_playtime_last_two_weeks"),
            pl.col("author_last_played").cast(pl.Int32).alias("author_last_played"),
            pl.col("timestamp_created").cast(pl.Int32).alias("timestamp_created"),
            pl.col("timestamp_updated").cast(pl.Int32).alias("timestamp_updated"),
            pl.col("voted_up").cast(pl.Int32).alias("voted_up"),
            pl.col("votes_up").cast(pl.Int32).alias("votes_up"),
            pl.col("steam_purchase").cast(pl.Int32).alias("steam_purchase"),
            pl.col("received_for_free").cast(pl.Int32).alias("received_for_free"),
            pl.col("written_during_early_access").cast(pl.Int32).alias("written_during_early_access")
            # pl.col("author_steamid").cast(pl.Int32).alias("author_steamid"),
            # pl.col("comment_count").cast(pl.Int32).alias("comment_count"),
            # pl.col("votes_funny").cast(pl.Int32).alias("votes_funny"),
            
        )
        
        # 4.9 b value for both comment count and votes_funny, must be downvoted comments (should be negative)
        df = df.with_columns([
            pl.when(pl.col(col) >= 2_147_483_647)  # max i32 value
            .then(pl.col(col) - 4_294_967_296)  # 2's comp wraparound
            .otherwise(pl.col(col))
            .cast(pl.Int32) 
            .alias(col)
            for col in ["comment_count", "votes_funny"]
        ])

        table = df.to_arrow()

        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(
                parquet_file, 
                schema=table.schema, 
                compression="zstd"
            )
        parquet_writer.write_table(table)

except Exception as e:
    print(f"Error processing row: {e}")

finally:
    if parquet_writer:
        parquet_writer.close()
    print(f"Successfully converted {csv_file} to {parquet_file}")
