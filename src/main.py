import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Import modules from the project
from data_ingestion.scraper import scrape_idos_delays
from data_ingestion.weather_api import fetch_weather_data
from data_processing.cleaner import DataCleaner
from analysis.statistical_model import StatisticalAnalyzer
from visualization.plotter import Plotter
from gis_output.spatial_processor import SpatialProcessor

# --- Configuration ---
# For a real application, API keys and configuration should be managed more securely
WEATHER_API_KEY = "YOUR_WEATHER_API_KEY" # Replace with your actual weather API key if needed
IDOS_BASE_URL = "https://www.idos.cz/vlaky/spojeni/" # Actual IDOS/GRAPP URL might be more specific

# Example coordinates for the region of interest (Ostrava)
REGION_LATITUDE = 49.8209
REGION_LONGITUDE = 18.2625

# Example railway line 323 stations for scraping
TRAIN_LINE_STATIONS = [
    {"from": "Ostrava hl.n.", "to": "Frýdlant n.O."},
    {"from": "Frýdlant n.O.", "to": "Čeladná"},
    {"from": "Čeladná", "to": "Frenštát p.R."},
]

def run_pipeline() -> None:
    """
    Orchestrates the entire train delay analysis pipeline.
    """
    print("--- Starting Train Delay Analysis Pipeline ---")

    data_cleaner = DataCleaner()
    statistical_analyzer = StatisticalAnalyzer()
    plotter = Plotter()
    spatial_processor = SpatialProcessor()

    # --- 1. Data Acquisition ---
    print("\n--- Phase 1: Data Acquisition ---")
    all_delay_data: list[Dict[str, Any]] = []
    
    # Simulate scraping for a few days
    # In a real scenario, this would be more robust and cover a longer period
    # and handle pagination/date ranges on IDOS.
    start_date = datetime.now() - timedelta(days=3)
    end_date = datetime.now()
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Scraping for date: {date_str}")
        for route in TRAIN_LINE_STATIONS:
            idos_params = {
                "f": route["from"],
                "t": route["to"],
                "date": current_date.strftime("%d.%m.%Y"),
                "time": "00:00" # Scrape for the whole day (adjust logic if needed for full day)
            }
            # Note: The scraper.py is a placeholder. Real scraping needs actual website analysis.
            # For demonstration, we'll use dummy data for `train_df_raw` later.
            # scraped_data = scrape_idos_delays(IDOS_BASE_URL, idos_params)
            # all_delay_data.extend(scraped_data)
        
        # Fetch weather data for the day
        if WEATHER_API_KEY != "YOUR_WEATHER_API_KEY":
            weather_data = fetch_weather_data(WEATHER_API_KEY, REGION_LATITUDE, REGION_LONGITUDE, date_str)
            if weather_data:
                # In a real scenario, process and save weather_data
                print(f"Fetched weather data for {date_str}")
            else:
                print(f"Failed to fetch weather data for {date_str}")
        else:
            print("Weather API key not configured. Skipping weather data fetch.")
        
        current_date += timedelta(days=1)

    # For demonstration, use dummy data as scraping is complex and site-dependent
    dummy_train_data = [
        {"train_id": "R123", "scheduled_time": "2023-01-15 08:00:00", "actual_time": "2023-01-15 08:05:00", "delay_minutes": 5, "route": "Ostrava-Frenstat", "date": "2023-01-15"},
        {"train_id": "EC456", "scheduled_time": "2023-01-15 10:30:00", "actual_time": "2023-01-15 10:30:00", "delay_minutes": 0, "route": "Ostrava-Frydlant", "date": "2023-01-15"},
        {"train_id": "R123", "scheduled_time": "2023-01-16 08:00:00", "actual_time": "2023-01-16 08:10:00", "delay_minutes": 10, "route": "Ostrava-Frenstat", "date": "2023-01-16"},
        {"train_id": "EC456", "scheduled_time": "2023-01-16 10:30:00", "actual_time": "2023-01-16 10:35:00", "delay_minutes": 5, "route": "Ostrava-Frydlant", "date": "2023-01-16"},
        {"train_id": "R123", "scheduled_time": "2023-01-17 08:00:00", "actual_time": "2023-01-17 08:00:00", "delay_minutes": 0, "route": "Ostrava-Frenstat", "date": "2023-01-17"},
        {"train_id": "R123", "scheduled_time": "2023-01-17 12:00:00", "actual_time": "2023-01-17 12:15:00", "delay_minutes": 15, "route": "Ostrava-Frenstat", "date": "2023-01-17"},
        {"train_id": "EC456", "scheduled_time": "2023-01-18 09:00:00", "actual_time": "2023-01-18 09:02:00", "delay_minutes": 2, "route": "Ostrava-Frydlant", "date": "2023-01-18"},
        {"train_id": "R123", "scheduled_time": "2023-01-18 14:00:00", "actual_time": "2023-01-18 14:20:00", "delay_minutes": 20, "route": "Ostrava-Frenstat", "date": "2023-01-18"},
    ]
    train_df_raw = pd.DataFrame(dummy_train_data)

    dummy_weather_data = [
        {"date": "2023-01-15", "temperature": 2.5, "humidity": 85, "wind_speed": 15, "precipitation": 0.5, "weather_condition": "cloudy"},
        {"date": "2023-01-16", "temperature": -1.0, "humidity": 90, "wind_speed": 20, "precipitation": 2.0, "weather_condition": "snowy"},
        {"date": "2023-01-17", "temperature": 5.0, "humidity": 70, "wind_speed": 10, "precipitation": 0.0, "weather_condition": "rainy"},
        {"date": "2023-01-18", "temperature": 3.0, "humidity": 75, "wind_speed": 12, "precipitation": 0.0, "weather_condition": "partly cloudy"},
    ]
    weather_df_raw = pd.DataFrame(dummy_weather_data)

    # --- 2. Data Processing and Cleaning ---
    print("\n--- Phase 2: Data Processing and Cleaning ---")
    train_df_cleaned = data_cleaner.clean_train_delays(train_df_raw.copy())
    weather_df_cleaned = data_cleaner.clean_weather_data(weather_df_raw.copy())
    
    merged_df = data_cleaner.merge_data(train_df_cleaned, weather_df_cleaned, on_column='date')
    
    if not merged_df.empty:
        print("Merged data head:")
        print(merged_df.head())
        merged_df.to_csv("data/processed/final_merged_data.csv", index=False)
        print("Processed data saved to data/processed/final_merged_data.csv")
    else:
        print("Error: Merged DataFrame is empty. Exiting pipeline.")
        return

    # --- 3. Statistical Analysis ---
    print("\n--- Phase 3: Statistical Analysis ---")
    print("Descriptive statistics for 'delay_minutes':")
    delay_stats = statistical_analyzer.get_descriptive_statistics(merged_df, 'delay_minutes')
    if delay_stats is not None:
        print(delay_stats)
    
    if 'weather_condition' in merged_df.columns:
        merged_df['is_snowy'] = merged_df['weather_condition'].apply(lambda x: 1 if x == 'snowy' else 0)
        t_test_results = statistical_analyzer.perform_t_test(merged_df, 'is_snowy', 'delay_minutes', 1, 0)
        if t_test_results:
            print(f"\nT-test (Snowy vs. Non-Snowy) - T-statistic: {t_test_results[0]:.2f}, P-value: {t_test_results[1]:.3f}")

    if 'temperature' in merged_df.columns:
        correlation = statistical_analyzer.calculate_correlation(merged_df, 'delay_minutes', 'temperature')
        if correlation is not None:
            print(f"\nCorrelation (Delay vs. Temperature): {correlation:.2f}")

    # --- 4. Visualizations ---
    print("\n--- Phase 4: Generating Visualizations ---")
    # Call plotting functions
    plotter.plot_delay_distribution(merged_df, 'delay_minutes')
    
    if 'scheduled_time' in merged_df.columns:
        daily_avg_delay = statistical_analyzer.aggregate_by_time(merged_df, 'scheduled_time', 'delay_minutes', freq='D', agg_func='mean')
        if not daily_avg_delay.empty:
            plotter.plot_time_series(daily_avg_delay, 'scheduled_time', 'delay_minutes', title='Daily Average Train Delay Minutes')

    numeric_cols_for_heatmap = ['delay_minutes', 'temperature', 'humidity']
    plotter.plot_correlation_heatmap(merged_df, columns=numeric_cols_for_heatmap)
    
    if 'weather_condition' in merged_df.columns:
        plotter.plot_category_vs_delay(merged_df, 'weather_condition', title='Average Delay by Weather Condition')

    # --- 5. GIS Outputs ---
    print("\n--- Phase 5: Generating GIS Outputs ---")
    # Example station data with placeholder coordinates (these would come from a lookup or GeoCoding)
    station_points = [
        {"station_name": "Ostrava hl.n.", "latitude": 49.8465, "longitude": 18.2917},
        {"station_name": "Frydlant n.O.", "latitude": 49.6645, "longitude": 18.3582},
        {"station_name": "Celadna", "latitude": 49.5760, "longitude": 18.3615},
        {"station_name": "Frenstat p.R.", "latitude": 49.5601, "longitude": 18.2140},
    ]
    df_stations_geo = pd.DataFrame(station_points)
    
    # Add average delay to station data for visualization purposes
    avg_delay_by_route = merged_df.groupby('route')['delay_minutes'].mean().reset_index()
    
    # Map average delays to stations based on a simplified route association
    def get_avg_delay_for_station(station_name: str, avg_delays_df: pd.DataFrame) -> float:
        if "Ostrava" in station_name or "Frenstat" in station_name or "Celadna" in station_name:
            return avg_delays_df[avg_delays_df['route'] == 'Ostrava-Frenstat']['delay_minutes'].iloc[0] if 'Ostrava-Frenstat' in avg_delays_df['route'].values else 0
        elif "Frydlant" in station_name:
            return avg_delays_df[avg_delays_df['route'] == 'Ostrava-Frydlant']['delay_minutes'].iloc[0] if 'Ostrava-Frydlant' in avg_delays_df['route'].values else 0
        return 0.0

    df_stations_geo['avg_delay'] = df_stations_geo['station_name'].apply(lambda x: get_avg_delay_for_station(x, avg_delay_by_route))

    gdf_stations = spatial_processor.create_gdf_from_points(df_stations_geo, 'latitude', 'longitude')
    
    if gdf_stations is not None:
        spatial_processor.save_gdf_to_shapefile(gdf_stations, "data/processed/train_stations_delays.shp")
        spatial_processor.create_leaflet_geojson(gdf_stations, "docs/leaflet_layers/train_stations_delays.geojson")
        print("Generated train_stations_delays.shp and train_stations_delays.geojson")
    else:
        print("Failed to generate GIS outputs for stations.")

    # --- 6. Optional: Launch Streamlit Dashboard ---
    print("\n--- Optional: Launching Streamlit Dashboard ---")
    print("To run the Streamlit dashboard, execute: streamlit run src/streamlit_app/app.py")
    print("Make sure you have an active weather API key in src/streamlit_app/app.py if you intend to fetch real data.")
    
    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    run_pipeline()
