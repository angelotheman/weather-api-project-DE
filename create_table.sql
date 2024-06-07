-- Create Current Weather Data Table
CREATE TABLE current_weather (
    city_name VARCHAR(50),
    country_code VARCHAR(2),
    temperature DECIMAL,
    min_temperature DECIMAL,
    max_temperature DECIMAL,
    pressure INTEGER,
    humidity INTEGER,
    wind_speed DECIMAL,
    weather_description VARCHAR(255),
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    timestamp TIMESTAMP
);

-- Create Forecast Weather Data Table
CREATE TABLE forecast_weather (
    city_name VARCHAR(50),
    date DATE,
    temperature DECIMAL,
    min_temperature DECIMAL,
    max_temperature DECIMAL,
    pressure INTEGER,
    humidity INTEGER,
    wind_speed DECIMAL,
    weather_description VARCHAR(255)
);
