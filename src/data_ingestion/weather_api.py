import requests
from typing import Dict, Any, Optional

def fetch_weather_data(api_key: str, latitude: float, longitude: float, date: str) -> Optional[Dict[str, Any]]:
    """
    Fetches weather data for a specific location and date from a weather API.
    
    Args:
        api_key (str): Your weather API key.
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        date (str): Date in 'YYYY-MM-DD' format for historical data.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing weather data, or None if an error occurs.
    """
    # This is a placeholder for a real weather API.
    # You would replace this with an actual API endpoint and parameters
    # (e.g., OpenWeatherMap, WeatherAPI.com, etc.).
    # For historical data, you might need a different endpoint or a paid plan.
    
    # Example using a hypothetical historical weather API endpoint
    base_url = "http://api.weather-service.com/history" 
    
    params = {
        "key": api_key,
        "lat": latitude,
        "lon": longitude,
        "date": date,
        "units": "metric" # or "imperial"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        weather_data = response.json()
        return weather_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None
    except ValueError as e:
        print(f"Error parsing weather data (invalid JSON): {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    # Example usage:
    # Replace with your actual API key and desired location/date
    
    # For a real project, store API keys securely (e.g., environment variables)
    WEATHER_API_KEY = "YOUR_WEATHER_API_KEY" 
    
    # Coordinates for Ostrava, Czech Republic (example)
    ostrava_lat = 49.8209
    ostrava_lon = 18.2625
    
    target_date = "2023-01-15" # Example historical date

    if WEATHER_API_KEY == "YOUR_WEATHER_API_KEY":
        print("Please replace 'YOUR_WEATHER_API_KEY' with your actual weather API key.")
    else:
        print(f"Fetching weather data for {ostrava_lat}, {ostrava_lon} on {target_date}...")
        weather_info = fetch_weather_data(WEATHER_API_KEY, ostrava_lat, ostrava_lon, target_date)

        if weather_info:
            print("Weather Data:")
            print(weather_info)
            # In a real scenario, you'd process and save this data
            # For example, to data/raw/weather_data_YYYY-MM-DD.json
        else:
            print("Failed to fetch weather data.")
