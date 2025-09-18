import pandas as pd

# Path to your interim or raw dataset
file_path = "data/raw/recipes.csv"

# Load just a few rows for inspection
df = pd.read_csv(file_path, nrows=5)

# Print 2 sample rows
print("=== Sample Rows ===")
print(df.head(2)["TotalTime"])

# Print column datatypes
print("\n=== Column Data Types ===")
print(df.dtypes)
