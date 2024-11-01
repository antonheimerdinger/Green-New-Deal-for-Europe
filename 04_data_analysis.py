import pandas as pd

pd.set_option('display.max_rows', None)

df = pd.read_parquet('combined_data/cleaned_data.parquet', engine='pyarrow')

print(df.shape)
print(df.columns)
print(df.isna().sum())
