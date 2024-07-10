import pandas as pd
import os
import pyarrow.parquet as pq

INPUT_DIR = r"data"
OUTPUT_FILE = "processed_data.csv"

def clean_and_transform(df):
    df = df.dropna(how=all)
    df['trip_duration'] = (pd.to_datetime(df['tpep_dropoff_datetime']) - pd.to_datetime(df['tpep_pickup_datetime'])).dt.total_seconds() / 60
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)
    return df

def main():
    all_data = []
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".parquet"):
            file_path = os.path.join(INPUT_DIR, file)
            try:
                df = pq.read_table(file_path).to_pandas()
                df = clean_and_transform(df)
                all_data.append(df)
                print(f"Processed {file}")
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
    
    if all_data:
        combined_df = pd.concat(all_data)
        try:
            combined_df.to_csv(OUTPUT_FILE, index=False)
            print(f"Processed data saved to {OUTPUT_FILE}")
        except Exception as e:
            print(f"Error saving to CSV: {str(e)}")
    else:
        print("No data processed.")

if __name__ == "__main__":
    main()
