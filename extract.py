#!/usr/bin/env python3
"""
Extract 3-hour interval weather data from OpenWeatherMap API
and save to Amazon S3.
"""
from os import getenv
import requests
import csv
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime, timezone
import time
import boto3
import pandas as pd
import io

# Load environment variables
load_dotenv()

api_key = getenv("API_KEY")

# AWS S3 details
AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = getenv("S3_BUCKET")
FOLDER_NAME = getenv("S3_FOLDER")

# List of cities to work on
cities = [
    'Accra', 'Kumasi', 'Tamale', 'Sunyani',
    'Cape Coast', 'Sekondi-Takoradi', 'Kasoa',
    'Obuasi', 'Tema'
]


def get_3hour_forecast(city):
    """
    Get 3-hour interval weather data.
    """
    api_url = 'http://api.openweathermap.org/data/2.5/forecast'
    city_url = f"{api_url}?q={city}&appid={api_key}&units=metric"
    response = requests.get(city_url)
    if response.status_code == 200:
        data = response.json()
        forecast_data = []
        for forecast in data['list']:
            dt = datetime.utcfromtimestamp(forecast['dt']).strftime(
                    '%Y-%m-%d %H:%M:%S')
            forecast_data.append({
                'city_name': city,
                'datetime': dt,
                'temperature': forecast['main']['temp'],
                'min_temperature': forecast['main']['temp_min'],
                'max_temperature': forecast['main']['temp_max'],
                'pressure': forecast['main']['pressure'],
                'humidity': forecast['main']['humidity'],
                'wind_speed': forecast['wind']['speed'],
                'weather_description': forecast['weather'][0]['description'],
                'cloudiness': forecast['clouds']['all'],
                'precipitation': forecast.get('rain', {}).get('3h', 0) +
                forecast.get('snow', {}).get('3h', 0),
            })
        return forecast_data
    else:
        print(
            "Error fetching 3-hour forecast for {}: {}"
            .format(city, response.status_code)
        )
        return []


def save_to_s3(csv_buffer, filename):
    """
    Save the CSV to Amazon S3.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3.put_object(
            Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/{filename}",
            Body=csv_buffer.getvalue()
    )


def main():
    forecast_weather_data = []
    for city in tqdm(cities, desc="Fetching 3-hour interval weather data"):
        forecast_weather_data.extend(get_3hour_forecast(city))
        time.sleep(1)

    # Convert forecast weather data to DataFrame
    forecast_df = pd.DataFrame(forecast_weather_data)

    # Save forecast weather data to CSV
    forecast_csv_buffer = io.StringIO()
    forecast_df.to_csv(forecast_csv_buffer, index=False)
    save_to_s3(forecast_csv_buffer, '3hour_interval_weather_data.csv')
    print(f"3-hour interval weather data saved to S3")


if __name__ == '__main__':
    main()
