import os
import requests
from urllib.parse import urljoin

BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'
DOWNLOAD_DIR = "data"

def download_data(year, month):
    file_name = f'{year}-{month:02}.parquet'
    url = f'{BASE_URL}{file_name}'
    response = requests.get(url)
    response.raise_for_status()
    
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    
    file_path = os.path.join(DOWNLOAD_DIR,file_name)
    with open(file_path, "wb") as f:
        f.write(response.content)

    print(f"Downloaded: {file_path}")

def main():
    year = 2019
    for month in range(1, 13):
        try:
            download_data(year, month)
        except requests.RequestException as e:
            print(f"Failed to download data for {year}-{month:02d}: {e}")

if __name__ == "__main__":
    main()
