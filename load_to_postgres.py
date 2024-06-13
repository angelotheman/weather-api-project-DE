#!/usr/bin/env python3
"""
Load the transformed scripts to postgres
"""
from os import getenv
import psycopg2
import pandas as pd
from dotenv import load_dotenv


load_dotenv()


dbname = getenv('POSTGRES_DB')
user = getenv('POSTGRES_USER')
password = getenv('POSTGRES_PWD')
host = getenv('POSTGRES_HOST')
port = getenv('POSTGRES_PORT')


# Connect to Postgre SQL
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    print("Connected to Postgres SQL successfully")
except psycopg2.Error as e:
    print(f"Error connecting to PostgreSQL: {e}")


create_table_sql = """
CREATE TABLE IF NOT EXISTS weather_data (
    city_name VARCHAR(100),
    datetime TIMESTAMP,
    temperature FLOAT,
    min_temperature FLOAT,
    max_temperature FLOAT,
    pressure INT,
    humidity INT,
    wind_speed FLOAT,
    weather_description VARCHAR(100),
    cloudiness INT,
    precipitation FLOAT,
    date DATE,
    time TIME,
    temp_range FLOAT
);
"""

try:
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()
    print("weather_data table created successfully!")
except psycopg2.Error as e:
    conn.rollback()
    print(f"Error creating weather_data table: {e}")

file_path = 'transformations/transformed_weather_data.csv'
df = pd.read_csv(file_path)


data_tuples = [tuple(row) for row in df.to_numpy()]

# Prepare SQL query for insertion
insert_query = """
    INSERT INTO weather_data (
        city_name, datetime, temperature, min_temperature, max_temperature,
        pressure, humidity, wind_speed, weather_description, cloudiness,
        precipitation, date, time, temp_range
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# Execute insertion
try:
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, data_tuples)
    conn.commit()
    print(f"{len(data_tuples)} rows inserted into weather_data table.")
except psycopg2.Error as e:
    conn.rollback()
    print(f"Error inserting rows into weather_data table: {e}")
finally:
    conn.close()
    print("PostgreSQL connection closed.")
