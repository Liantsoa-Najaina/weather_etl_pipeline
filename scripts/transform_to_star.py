import os
import logging
import pandas as pd


def transform_to_star() -> bool:
    """
    Takes the big chunk of dataset and separate the colum to obtain fact and dimensions tables
    :return: True if successful, False otherwise
    """
    try:
        input_file =  "/home/najaina/data/weather_tourism_dataset.csv"
        output_dir = "/home/najaina/data/star_schema/"

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Load dataset into a dataframe
        df = pd.read_csv(input_file)

        # Create dimension tables
        dim_city = df[['city']].drop_duplicates().reset_index(drop=True)
        dim_city['city_id'] = dim_city.index + 1

        dim_description = df[['description']].drop_duplicates().reset_index(drop=True)
        dim_description['description_id'] = dim_description.index + 1

        df['date'] = pd.to_datetime(df['date'])
        dim_date = df[['date']].drop_duplicates().reset_index(drop=True)
        dim_date['date_id'] = dim_date.index + 1
        dim_date['year'] = dim_date['date'].dt.year
        dim_date['month'] = dim_date['date'].dt.month
        dim_date['day'] = dim_date['date'].dt.day
        dim_date['weekday'] = dim_date['date'].dt.day_name()

        # Merge into fact table
        df_star = df.merge(dim_city, on='city').merge(dim_description, on='description').merge(dim_date, on='date')

        fact_weather = df_star[[
            'city_id', 'date_id', 'temperature', 'feels_like',
            'humidity', 'description_id', 'wind_speed'
        ]]

        # Save into cs files
        dim_city.to_csv(f"{output_dir}/dim_city.csv", index=False)
        dim_description.to_csv(f"{output_dir}/dim_weather_description.csv", index=False)
        dim_date.to_csv(f"{output_dir}/dim_date.csv", index=False)
        fact_weather.to_csv(f"{output_dir}/fact_weather.csv", index=False)

        return True
    except Exception as e:
        logging.error(f"Transformation to star schema failed: {e}")
    return False

