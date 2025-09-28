import pandas as pd

# Check the source file
df_src = pd.read_parquet("data/processed/recipes_clean.parquet")
print("Source rows:", len(df_src))
print("Columns:", df_src.columns.tolist())
print(df_src.head())

df_chunks = pd.read_parquet("data/processed/chunks.parquet")
print("Chunks rows:", len(df_chunks))
print("Columns:", df_chunks.columns.tolist())
print(df_chunks.head())
