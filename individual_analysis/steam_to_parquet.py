import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl

csv_file = "all_reviews/all_reviews.csv"
parquet_file = "all_reviews/all_reviews.parquet"

BLOCK_SIZE = 10_000_000

read_options = pv.ReadOptions(block_size=BLOCK_SIZE)
csv_reader = pv.open_csv(csv_file, read_options=read_options)

parquet_writer = None

try:
    for record_batch in csv_reader:
        print(f"Processing batch with {record_batch.num_rows} rows...")

        df = pl.from_arrow(record_batch)

        df = df.drop(["steam_china_location", "hidden_in_steam_china", "recommendationid"])

        print("First 10 values of weighted_vote_score:")
        print(df["weighted_vote_score"][:10])
        

        print("Columns:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
        
        break

        table = df.to_arrow()

        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(
                parquet_file, 
                schema=table.schema, 
                compression="zstd"
            )
        parquet_writer.write_table(table)

finally:
    if parquet_writer:
        parquet_writer.close()

print(f"Successfully converted {csv_file} to {parquet_file}")