#!/usr/bin/env python3
"""
Run this transformation against the extraction scripts
"""
from os import getenv
import boto3
import pandas as pd
import io
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv


load_dotenv()
# AWS S3 Details
AWS_ACCESS_KEY_ID = getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = ('S3_BUCKET')
FOLDER_NAME = ('S3_FOLDER')


# Read data from s3 bucket
def read_from_s3(filename):
    """
    Read the CSV from Amazon S3.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    try:
        obj = s3.get_object(
            Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/raw_data/{filename}"
        )
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    except s3.exceptions.NoSuchKey:
        print("File not found in S3")
        return pd.DataFrame()


# Load the data
filename = '3hour_interval_weather_data.csv'
df = read_from_s3(filename)

# Convert datetime column to datetime type
df['datetime'] = pd.to_datetime(df['datetime'])

# Extract date and time from datetime
df['date'] = df['datetime'].dt.date
df['time'] = df['datetime'].dt.time

# Calculate additional features
df['temp_range'] = df['max_temperature'] - df['min_temperature']

# Convert pressure and humidity columns to float type
df['pressure'] = df['pressure'].astype(float)
df['humidity'] = df['humidity'].astype(float)

# Save the transformed data to a CSV file locally
transformed_csv_filename = 'transformed_weather_data.csv'
df.to_csv(transformed_csv_filename, index=False)
print(f"Transformed data saved to {transformed_csv_filename} locally.")
