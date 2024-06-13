#!/usr/bin/env python3
"""
Extract 3-hour interval weather data from OpenWeatherMap API
and save to a local CSV file and S3 bucket.
"""
from os import getenv
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime
import time
import pandas as pd
import boto3
import io

# Load environment variables
load_dotenv()

api_key = getenv("API_KEY")
if not api_key:
    raise ValueError("API_KEY not found. Please set it in the .env file.")

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
    print(f"Fetching data for {city} using URL: {city_url}")  # Debugging statement
    response = requests.get(city_url)
    if response.status_code == 200:
        data = response.json()
        forecast_data = []
        # Iterate through the forecast list
        for forecast in data['list']:
            dt = datetime.utcfromtimestamp(forecast['dt']).strftime('%Y-%m-%d %H:%M:%S')
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
        print(f"Collected {len(forecast_data)} forecasts for {city}")
        return forecast_data
    else:
        print(f"Error fetching 3-hour forecast for {city}: {response.status_code}")
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
        city_data = get_3hour_forecast(city)
        forecast_weather_data.extend(city_data)
        time.sleep(1)  # To avoid hitting the API rate limit

    # Convert forecast weather data to DataFrame
    forecast_df = pd.DataFrame(forecast_weather_data)

    # Save forecast weather data to a local CSV file
    local_filename = 'raw_data/3hour_interval_weather_data.csv'
    forecast_df.to_csv(local_filename, index=False)
    print(f"Data saved to {local_filename}")

    # Save forecast weather data to S3
    csv_buffer = io.StringIO()
    forecast_df.to_csv(csv_buffer, index=False)
    save_to_s3(csv_buffer, local_filename)
    print(f"Data uploaded to S3 bucket {BUCKET_NAME} in folder {FOLDER_NAME}")


if __name__ == '__main__':
    main()
