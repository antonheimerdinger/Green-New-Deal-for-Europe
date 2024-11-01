import pandas as pd
import os

directory = ['EinheitenSolar', 'AnlagenEegSolar']
os.makedirs('combined_data', exist_ok=True)

for type in directory:
    # Define the directory where parquet files are stored
    data_folder = f"data/{type}"

    # List all files in the directory
    files = [f for f in os.listdir(data_folder)]

    # Read all parquet files and combine them into a single DataFrame
    df_list = [pd.read_parquet(os.path.join(data_folder, file)) for file in files]
    combined_df = pd.concat(df_list, ignore_index=True)

    output_path = f'combined_data/{type}.parquet'
    combined_df.to_parquet(output_path, index=False)
