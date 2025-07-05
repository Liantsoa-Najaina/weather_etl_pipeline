import requests
import pandas as pd
import os
import logging


def extract_weather(city: str, api_key: str, date: str) -> bool:
    """
    Extract weather data for a given city and date with OpenWeatherMap API
    :param city: name of the city
    :param api_key: api key for OpenWeatherMap API
    :param date: the date to extract weather data for
    :return: True if weather data was extracted successfully, False otherwise
    """
    try:
        # Request configuration
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
            'lang': 'en'
        }

        # Sending the request with a 60s timeout
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 200:
            print("weather data extracted successfully for city: " + city)
        else:
            response.raise_for_status()

        # Pertinent fields extraction
        data = response.json()
        weather_data = {
            'city': city,
            'date': date,
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'description': data['weather'][0]['description'],
            'wind_speed': data['wind']['speed'],
            'clouds': data['clouds']['all'],
        }

        # Creating folder to dump collected data
        os.makedirs(f"/home/najaina/data/raw/{date}", exist_ok=True)

        # Saving data to csv
        pd.DataFrame([weather_data]).to_csv(
            f"/home/najaina/data/raw/{date}/weather_{city}.csv",
            index=False
        )

        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Network or API request error for {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Missing field in response for {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error for {city}: {str(e)}")
    return False



