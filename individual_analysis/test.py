import pyarrow.csv as pv
import pyarrow.parquet as pq
import polars as pl

csv_file = "all_reviews/all_reviews.csv"
parquet_file = "all_reviews/all_reviews.parquet"

# Read the entire CSV file into a record batch
record_batch = pv.read_csv(csv_file)

# Convert the record batch to a Polars DataFrame
df = pl.from_arrow(record_batch)

# Drop unnecessary columns
df = df.drop(["steam_china_location", "hidden_in_steam_china", "recommendationid"])

# Process columns
df = df.with_columns(
    pl.col("weighted_vote_score").cast(pl.Float32).alias("weighted_vote_score"),
    pl.col("appid").cast(pl.Utf8).alias("appid"),
    pl.col("author_num_games_owned").cast(pl.Int32).alias("author_num_games_owned"),
    pl.col("author_num_reviews").cast(pl.Int32).alias("author_num_reviews"),
    pl.col("author_playtime_forever").cast(pl.Int32).alias("author_playtime_forever"),
    pl.col("author_playtime_last_two_weeks").cast(pl.Int32).alias("author_playtime_last_two_weeks"),
    pl.col("author_last_played").cast(pl.Int32).alias("author_last_played"),
    pl.col("timestamp_created").cast(pl.Int32).alias("timestamp_created"),
    pl.col("timestamp_updated").cast(pl.Int32).alias("timestamp_updated"),
    pl.col("voted_up").cast(pl.Int32).alias("voted_up"),
    pl.col("votes_up").cast(pl.Int32).alias("votes_up"),
    pl.col("comment_count").cast(pl.Int32).alias("comment_count"),
    pl.col("steam_purchase").cast(pl.Int32).alias("steam_purchase"),
    pl.col("received_for_free").cast(pl.Int32).alias("received_for_free"),
    pl.col("written_during_early_access").cast(pl.Int32).alias("written_during_early_access")
)

# Convert the DataFrame to an Arrow table
table = df.to_arrow()

# Write the entire table to a Parquet file
parquet_writer = pq.ParquetWriter(
    parquet_file, 
    schema=table.schema, 
    compression="zstd"
)
parquet_writer.write_table(table)

# Close the writer
parquet_writer.close()

print(f"Successfully converted {csv_file} to {parquet_file}")