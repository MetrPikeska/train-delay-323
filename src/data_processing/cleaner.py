import pandas as pd
from typing import Dict, Any, List

class DataCleaner:
    """
    A class for cleaning and preprocessing train delay and weather datasets.
    """

    def __init__(self):
        """
        Initializes the DataCleaner.
        """
        pass

    def clean_train_delays(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and preprocesses the train delay DataFrame.
        
        Args:
            df (pd.DataFrame): Raw train delay data.

        Returns:
            pd.DataFrame: Cleaned train delay data.
        """
        # Example cleaning steps. These will need to be adapted based on actual data.
        
        # 1. Convert relevant columns to appropriate data types
        try:
            if 'delay_minutes' in df.columns:
                df['delay_minutes'] = pd.to_numeric(df['delay_minutes'], errors='coerce')
            if 'scheduled_time' in df.columns:
                df['scheduled_time'] = pd.to_datetime(df['scheduled_time'], errors='coerce')
            if 'actual_time' in df.columns:
                df['actual_time'] = pd.to_datetime(df['actual_time'], errors='coerce')
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
        except KeyError as e:
            print(f"Warning: Column {e} not found for type conversion in train delays.")
        
        # 2. Handle missing values (example: fill with 0 for delays, or drop rows)
        if 'delay_minutes' in df.columns:
            df['delay_minutes'] = df['delay_minutes'].fillna(0)
            
        # 3. Remove duplicates
        df.drop_duplicates(inplace=True)
        
        # 4. Feature engineering (example: calculate day of week, hour of day)
        if 'scheduled_time' in df.columns:
            df['day_of_week'] = df['scheduled_time'].dt.dayofweek
            df['hour_of_day'] = df['scheduled_time'].dt.hour

        return df

    def clean_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and preprocesses the weather data DataFrame.
        
        Args:
            df (pd.DataFrame): Raw weather data.

        Returns:
            pd.DataFrame: Cleaned weather data.
        """
        # Example cleaning steps. These will need to be adapted based on actual data.
        
        # 1. Convert relevant columns to appropriate data types
        try:
            if 'temperature' in df.columns:
                df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
            if 'humidity' in df.columns:
                df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
            if 'wind_speed' in df.columns:
                df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce')
            if 'precipitation' in df.columns:
                df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
        except KeyError as e:
            print(f"Warning: Column {e} not found for type conversion in weather data.")
            
        # 2. Handle missing values (example: fill with mean, median, or drop rows)
        # For simplicity, let's just drop rows with any missing weather data for now
        df.dropna(inplace=True) 
        
        return df

    def merge_data(self, train_df: pd.DataFrame, weather_df: pd.DataFrame, 
                   on_column: str = 'date', how: str = 'left') -> pd.DataFrame:
        """
        Merges train delay data with weather data.
        
        Args:
            train_df (pd.DataFrame): Cleaned train delay data.
            weather_df (pd.DataFrame): Cleaned weather data.
            on_column (str): Column to merge on (e.g., 'date').
            how (str): Type of merge to be performed.

        Returns:
            pd.DataFrame: Merged DataFrame.
        """
        try:
            merged_df = pd.merge(train_df, weather_df, on=on_column, how=how)
            return merged_df
        except KeyError as e:
            print(f"Error merging data: Missing key '{e}' in one of the DataFrames.")
            return pd.DataFrame()
        except Exception as e:
            print(f"An unexpected error occurred during merging: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Example usage:
    cleaner = DataCleaner()
    
    # Create dummy dataframes for demonstration
    dummy_train_data: List[Dict[str, Any]] = [
        {"train_id": "R123", "scheduled_time": "2023-01-15 08:00:00", "actual_time": "2023-01-15 08:05:00", "delay_minutes": 5, "route": "Ostrava-Frenstat", "date": "2023-01-15"},
        {"train_id": "EC456", "scheduled_time": "2023-01-15 10:30:00", "actual_time": "2023-01-15 10:30:00", "delay_minutes": 0, "route": "Ostrava-Frydlant", "date": "2023-01-15"},
        {"train_id": "R123", "scheduled_time": "2023-01-16 08:00:00", "actual_time": "2023-01-16 08:10:00", "delay_minutes": 10, "route": "Ostrava-Frenstat", "date": "2023-01-16"},
        {"train_id": "EC456", "scheduled_time": "2023-01-16 10:30:00", "actual_time": "2023-01-16 10:35:00", "delay_minutes": 5, "route": "Ostrava-Frydlant", "date": "2023-01-16"},
        {"train_id": "R123", "scheduled_time": "2023-01-17 08:00:00", "actual_time": "2023-01-17 08:00:00", "delay_minutes": None, "route": "Ostrava-Frenstat", "date": "2023-01-17"}, # Missing delay
    ]
    
    dummy_weather_data: List[Dict[str, Any]] = [
        {"date": "2023-01-15", "temperature": 2.5, "humidity": 85, "wind_speed": 15, "precipitation": 0.5},
        {"date": "2023-01-16", "temperature": -1.0, "humidity": 90, "wind_speed": 20, "precipitation": 2.0},
        {"date": "2023-01-17", "temperature": 5.0, "humidity": 70, "wind_speed": 10, "precipitation": 0.0},
        {"date": "2023-01-18", "temperature": 3.0, "humidity": 75, "wind_speed": 12, "precipitation": 0.0}, # No corresponding train data
    ]

    train_df_raw = pd.DataFrame(dummy_train_data)
    weather_df_raw = pd.DataFrame(dummy_weather_data)
    
    print("Original Train Data:")
    print(train_df_raw)
    print("\nOriginal Weather Data:")
    print(weather_df_raw)

    train_df_cleaned = cleaner.clean_train_delays(train_df_raw.copy())
    weather_df_cleaned = cleaner.clean_weather_data(weather_df_raw.copy())
    
    print("\nCleaned Train Data:")
    print(train_df_cleaned)
    print("\nCleaned Weather Data:")
    print(weather_df_cleaned)

    merged_df = cleaner.merge_data(train_df_cleaned, weather_df_cleaned, on_column='date')
    print("\nMerged Data:")
    print(merged_df)
    
    merged_df.to_csv("../../data/processed/merged_delays_weather.csv", index=False)
    print("Merged data saved to data/processed/merged_delays_weather.csv")
