import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DATABASE = "taxi_data.db"

def query_database(conn, query):
    return pd.read_sql_query(query, conn)

def generate_reports():
    conn = sqlite3.connect(DATABASE)
    
    # Peak hours for taxi usage
    peak_hours_query = """
    SELECT strftime('%H', tpep_pickup_datetime) AS hour, COUNT(*) AS total_trips
    FROM trips
    GROUP BY hour
    ORDER BY total_trips DESC;
    """
    peak_hours = query_database(conn, peak_hours_query)
    peak_hours.plot(kind='bar', x='hour', y='total_trips', title='Peak Hours for Taxi Usage')
    plt.show()
    
    # Passenger count vs trip fare
    passenger_fare_query = """
    SELECT passenger_count, AVG(total_amount) AS avg_fare
    FROM trips
    GROUP BY passenger_count
    ORDER BY passenger_count;
    """
    passenger_fare = query_database(conn, passenger_fare_query)
    passenger_fare.plot(kind='bar', x='passenger_count', y='avg_fare', title='Passenger Count vs Average Trip Fare')
    plt.show()
    
    # Trends in usage over the year
    monthly_trends_query = """
    SELECT strftime('%Y-%m', tpep_pickup_datetime) AS month, COUNT(*) AS total_trips
    FROM trips
    GROUP BY month
    ORDER BY month;
    """
    monthly_trends = query_database(conn, monthly_trends_query)
    monthly_trends.plot(kind='line', x='month', y='total_trips', title='Monthly Taxi Usage Trends')
    plt.show()
    
    conn.close()

if __name__ == "__main__":
    generate_reports()