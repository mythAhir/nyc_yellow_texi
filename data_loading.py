import sqlite3
import pandas as pd

DATABASE = "taxi_data.db"
PROCESSED_FILE = "processed_data.csv"

def create_schema(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trips (
        VendorID INTEGER,
        tpep_pickup_datetime TEXT,
        tpep_dropoff_datetime TEXT,
        passenger_count INTEGER,
        trip_distance REAL,
        RatecodeID INTEGER,
        store_and_fwd_flag TEXT,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        payment_type INTEGER,
        fare_amount REAL,
        extra REAL,
        mta_tax REAL,
        tip_amount REAL,
        tolls_amount REAL,
        improvement_surcharge REAL,
        total_amount REAL,
        congestion_surcharge REAL,
        trip_duration REAL,
        average_speed REAL
    );
    """
    conn.execute(create_table_query)
    conn.commit()

def load_data(conn, csv_file):
    dtype = {
        'VendorID': 'int32',
        'tpep_pickup_datetime': 'str',
        'tpep_dropoff_datetime': 'str',
        'passenger_count': 'int32',
        'trip_distance': 'float64',
        'RatecodeID': 'int32',
        'store_and_fwd_flag': 'str',
        'PULocationID': 'int32',
        'DOLocationID': 'int32',
        'payment_type': 'int32',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'congestion_surcharge': 'float64',
        'trip_duration': 'float64',
        'average_speed': 'float64'
    }

    try:
        df = pd.read_csv(csv_file, dtype=dtype, low_memory=False)
        df.to_sql('trips', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"Error loading data: {e}")

def main():
    try:
        conn = sqlite3.connect(DATABASE)
        create_schema(conn)
        load_data(conn, PROCESSED_FILE)
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
    print(f"Data loaded into {DATABASE}")

if __name__ == "__main__":
    main()
