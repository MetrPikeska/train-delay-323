import streamlit as st
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, List, Optional

# Assuming these modules are in the path or installed
from src.data_ingestion.scraper import scrape_idos_delays
from src.data_ingestion.weather_api import fetch_weather_data
from src.data_processing.cleaner import DataCleaner
from src.analysis.statistical_model import StatisticalAnalyzer
from src.visualization.plotter import Plotter
from src.gis_output.spatial_processor import SpatialProcessor

# --- Configuration ---
# For a real application, API keys and configuration should be managed more securely
WEATHER_API_KEY = "YOUR_WEATHER_API_KEY" # Replace with your actual weather API key

# Default coordinates for Ostrava, Czech Republic (for weather data fetching example)
DEFAULT_LATITUDE = 49.8209
DEFAULT_LONGITUDE = 18.2625

# Initialize classes
data_cleaner = DataCleaner()
statistical_analyzer = StatisticalAnalyzer()
plotter = Plotter()
spatial_processor = SpatialProcessor()

st.set_page_config(layout="wide", page_title="Train Delay Analysis Dashboard")

def load_and_process_data() -> Optional[pd.DataFrame]:
    """
    Simulates loading and processing of data. In a real scenario, this would
    load from `data/raw` and `data/processed` directories.
    For this example, we use dummy data.
    """
    st.subheader("Data Acquisition & Processing (Dummy Data)")
    st.info("Using dummy data for demonstration. In a real scenario, this would involve scraping IDOS/GRAPP and fetching live weather data.")

    # Dummy train data
    dummy_train_data: List[Dict[str, Any]] = [
        {"train_id": "R123", "scheduled_time": "2023-01-15 08:00:00", "actual_time": "2023-01-15 08:05:00", "delay_minutes": 5, "route": "Ostrava-Frenstat", "date": "2023-01-15"},
        {"train_id": "EC456", "scheduled_time": "2023-01-15 10:30:00", "actual_time": "2023-01-15 10:30:00", "delay_minutes": 0, "route": "Ostrava-Frydlant", "date": "2023-01-15"},
        {"train_id": "R123", "scheduled_time": "2023-01-16 08:00:00", "actual_time": "2023-01-16 08:10:00", "delay_minutes": 10, "route": "Ostrava-Frenstat", "date": "2023-01-16"},
        {"train_id": "EC456", "scheduled_time": "2023-01-16 10:30:00", "actual_time": "2023-01-16 10:35:00", "delay_minutes": 5, "route": "Ostrava-Frydlant", "date": "2023-01-16"},
        {"train_id": "R123", "scheduled_time": "2023-01-17 08:00:00", "actual_time": "2023-01-17 08:00:00", "delay_minutes": 0, "route": "Ostrava-Frenstat", "date": "2023-01-17"},
        {"train_id": "R123", "scheduled_time": "2023-01-17 12:00:00", "actual_time": "2023-01-17 12:15:00", "delay_minutes": 15, "route": "Ostrava-Frenstat", "date": "2023-01-17"},
        {"train_id": "EC456", "scheduled_time": "2023-01-18 09:00:00", "actual_time": "2023-01-18 09:02:00", "delay_minutes": 2, "route": "Ostrava-Frydlant", "date": "2023-01-18"},
        {"train_id": "R123", "scheduled_time": "2023-01-18 14:00:00", "actual_time": "2023-01-18 14:20:00", "delay_minutes": 20, "route": "Ostrava-Frenstat", "date": "2023-01-18"},
    ]
    
    # Dummy weather data
    dummy_weather_data: List[Dict[str, Any]] = [
        {"date": "2023-01-15", "temperature": 2.5, "humidity": 85, "wind_speed": 15, "precipitation": 0.5, "weather_condition": "cloudy"},
        {"date": "2023-01-16", "temperature": -1.0, "humidity": 90, "wind_speed": 20, "precipitation": 2.0, "weather_condition": "snowy"},
        {"date": "2023-01-17", "temperature": 5.0, "humidity": 70, "wind_speed": 10, "precipitation": 0.0, "weather_condition": "rainy"},
        {"date": "2023-01-18", "temperature": 3.0, "humidity": 75, "wind_speed": 12, "precipitation": 0.0, "weather_condition": "partly cloudy"},
    ]

    train_df_raw = pd.DataFrame(dummy_train_data)
    weather_df_raw = pd.DataFrame(dummy_weather_data)

    st.write("Raw Train Data Sample:")
    st.dataframe(train_df_raw.head())
    st.write("Raw Weather Data Sample:")
    st.dataframe(weather_df_raw.head())

    train_df_cleaned = data_cleaner.clean_train_delays(train_df_raw.copy())
    weather_df_cleaned = data_cleaner.clean_weather_data(weather_df_raw.copy())
    
    st.write("Cleaned Train Data Sample:")
    st.dataframe(train_df_cleaned.head())
    st.write("Cleaned Weather Data Sample:")
    st.dataframe(weather_df_cleaned.head())

    merged_df = data_cleaner.merge_data(train_df_cleaned, weather_df_cleaned, on_column='date')
    st.success("Data loaded, cleaned, and merged!")
    return merged_df

def run_analysis_and_visualizations(df: pd.DataFrame) -> None:
    """
    Performs statistical analysis and generates visualizations.
    """
    st.header("Statistical Analysis & Visualizations")

    if 'delay_minutes' not in df.columns or df['delay_minutes'].empty:
        st.warning("Cannot perform analysis: 'delay_minutes' column is missing or empty.")
        return

    # --- Descriptive Statistics ---
    st.subheader("Descriptive Statistics for Train Delays")
    delay_stats = statistical_analyzer.get_descriptive_statistics(df, 'delay_minutes')
    if delay_stats is not None:
        st.write(delay_stats)
    else:
        st.error("Failed to calculate descriptive statistics.")

    # --- Delay Distribution Plot ---
    st.subheader("Distribution of Train Delay Minutes")
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    sns.histplot(df['delay_minutes'].dropna(), bins=30, kde=True, ax=ax1)
    ax1.set_title('Distribution of Train Delay Minutes')
    ax1.set_xlabel("Delay Minutes")
    ax1.set_ylabel("Frequency")
    st.pyplot(fig1)
    plt.close(fig1)

    # --- Time Series of Average Delays ---
    st.subheader("Time Series of Daily Average Train Delays")
    if 'scheduled_time' in df.columns and pd.api.types.is_datetime64_any_dtype(df['scheduled_time']):
        daily_avg_delay = statistical_analyzer.aggregate_by_time(df, 'scheduled_time', 'delay_minutes', freq='D', agg_func='mean')
        if not daily_avg_delay.empty:
            fig2, ax2 = plt.subplots(figsize=(12, 6))
            sns.lineplot(data=daily_avg_delay, x=daily_avg_delay.index, y='delay_minutes', ax=ax2)
            ax2.set_title('Daily Average Train Delay Minutes')
            ax2.set_xlabel("Date")
            ax2.set_ylabel("Average Delay Minutes")
            st.pyplot(fig2)
            plt.close(fig2)
        else:
            st.warning("Could not generate daily average delay time series.")
    else:
        st.warning("Missing 'scheduled_time' column or not in datetime format for time series plot.")

    # --- Correlation Heatmap ---
    st.subheader("Correlation Heatmap (Delay Minutes, Temperature, Humidity)")
    numeric_cols = ['delay_minutes', 'temperature', 'humidity', 'wind_speed', 'precipitation']
    available_numeric_cols = [col for col in numeric_cols if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
    if len(available_numeric_cols) > 1:
        fig3, ax3 = plt.subplots(figsize=(10, 8))
        sns.heatmap(df[available_numeric_cols].corr(), annot=True, cmap='coolwarm', fmt=".2f", ax=ax3)
        ax3.set_title('Correlation Heatmap')
        st.pyplot(fig3)
        plt.close(fig3)
    else:
        st.warning("Not enough numeric columns available for correlation heatmap.")

    # --- Average Delay by Weather Condition ---
    st.subheader("Average Delay by Weather Condition")
    if 'weather_condition' in df.columns:
        fig4, ax4 = plt.subplots(figsize=(10, 6))
        avg_delay_by_weather = df.groupby('weather_condition')['delay_minutes'].mean().reset_index()
        sns.barplot(data=avg_delay_by_weather, x='weather_condition', y='delay_minutes', ax=ax4)
        ax4.set_title('Average Delay by Weather Condition')
        ax4.set_xlabel('Weather Condition')
        ax4.set_ylabel('Average Delay Minutes')
        st.pyplot(fig4)
        plt.close(fig4)
    else:
        st.warning("Missing 'weather_condition' column for average delay analysis.")

    # --- T-test for Snowy vs. Non-Snowy Conditions ---
    st.subheader("T-test: Delays in Snowy vs. Non-Snowy Conditions")
    if 'weather_condition' in df.columns:
        df['is_snowy'] = df['weather_condition'].apply(lambda x: 1 if x == 'snowy' else 0)
        t_test_results = statistical_analyzer.perform_t_test(df, 'is_snowy', 'delay_minutes', 1, 0)
        if t_test_results:
            t_stat, p_val = t_test_results
            st.write(f"T-statistic: {t_stat:.2f}, P-value: {p_val:.3f}")
            if p_val < 0.05:
                st.write("Conclusion: There is a statistically significant difference in delay minutes between snowy and non-snowy conditions.")
            else:
                st.write("Conclusion: No statistically significant difference found in delay minutes between snowy and non-snowy conditions.")
        else:
            st.warning("Could not perform t-test. Check data for 'is_snowy' and 'delay_minutes'.")
    else:
        st.warning("Missing 'weather_condition' column for t-test analysis.")


def run_gis_outputs(df: pd.DataFrame) -> None:
    """
    Generates and displays GIS outputs.
    """
    st.header("GIS Outputs")

    # Dummy train station data with delays for GIS
    station_data: List[Dict[str, Any]] = [
        {"station_name": "Ostrava hl.n.", "latitude": 49.8465, "longitude": 18.2917, "avg_delay": df[df['route'].str.contains("Ostrava-Frenstat", na=False)]['delay_minutes'].mean() if not df[df['route'].str.contains("Ostrava-Frenstat", na=False)]['delay_minutes'].empty else 0},
        {"station_name": "Frydlant n.O.", "latitude": 49.6645, "longitude": 18.3582, "avg_delay": df[df['route'].str.contains("Ostrava-Frydlant", na=False)]['delay_minutes'].mean() if not df[df['route'].str.contains("Ostrava-Frydlant", na=False)]['delay_minutes'].empty else 0},
        {"station_name": "Celadna", "latitude": 49.5760, "longitude": 18.3615, "avg_delay": df[df['route'].str.contains("Ostrava-Frenstat", na=False)]['delay_minutes'].mean() if not df[df['route'].str.contains("Ostrava-Frenstat", na=False)]['delay_minutes'].empty else 0},
        {"station_name": "Frenstat p.R.", "latitude": 49.5601, "longitude": 18.2140, "avg_delay": df[df['route'].str.contains("Ostrava-Frenstat", na=False)]['delay_minutes'].mean() if not df[df['route'].str.contains("Ostrava-Frenstat", na=False)]['delay_minutes'].empty else 0},
    ]
    df_stations = pd.DataFrame(station_data)
    
    st.subheader("Train Stations GeoDataFrame")
    gdf_stations = spatial_processor.create_gdf_from_points(df_stations, 'latitude', 'longitude')
    if gdf_stations is not None:
        st.dataframe(gdf_stations.head())
        # Streamlit allows direct plotting of GeoDataFrames using st.map or pydeck
        st.map(gdf_stations)
        
        st.write("GeoJSON output for Leaflet:")
        geojson_str = gdf_stations.to_json()
        st.code(geojson_str[:500] + "...", language='json') # Show first 500 chars

        # Create dummy polygon data for spatial join demonstration
        poly1 = Polygon([(18.0, 49.5), (18.5, 49.5), (18.5, 50.0), (18.0, 50.0)])
        poly2 = Polygon([(18.5, 49.5), (19.0, 49.5), (19.0, 50.0), (18.5, 50.0)])
        district_data = {
            'district_name': ['Moravian-Silesian West', 'Moravian-Silesian East'],
            'geometry': [poly1, poly2]
        }
        gdf_districts = gpd.GeoDataFrame(district_data, crs="EPSG:4326")

        st.subheader("Spatial Join: Stations in Districts")
        joined_gdf = spatial_processor.spatial_join_data(gdf_stations, gdf_districts)
        if joined_gdf is not None:
            st.dataframe(joined_gdf.head())
            st.write("Note: In a real application, districts would be loaded from an actual shapefile.")
        else:
            st.warning("Failed to perform spatial join.")

    else:
        st.error("Failed to create GeoDataFrame from station data.")

# --- Streamlit App Layout ---
def main():
    st.title("Train Delay Analysis on Line 323")
    st.markdown("""
        This interactive dashboard provides insights into train delays on railway line 323, 
        integrating delay data with weather information and presenting geographical analyses.
    """)

    # --- Sidebar for Navigation ---
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Data Overview", "Analysis & Visualizations", "GIS Outputs", "Configuration"])

    if page == "Configuration":
        st.header("Configuration")
        st.warning("For this demonstration, API keys are hardcoded or dummy. In a production environment, use Streamlit Secrets or environment variables.")
        st.text_input("Weather API Key", value=WEATHER_API_KEY, type="password", help="Enter your actual weather API key here.")
        st.info("Currently, data fetching is simulated with dummy data. Real-time scraping and API calls would be enabled with proper configuration.")

    else:
        # Load and process data (only once)
        processed_data = load_and_process_data()

        if processed_data is not None and not processed_data.empty:
            if page == "Data Overview":
                st.header("Processed and Merged Data Overview")
                st.dataframe(processed_data)
                st.write(f"Total records: {len(processed_data)}")
                st.write(f"Columns: {processed_data.columns.tolist()}")

            elif page == "Analysis & Visualizations":
                run_analysis_and_visualizations(processed_data)

            elif page == "GIS Outputs":
                run_gis_outputs(processed_data)
        else:
            st.error("No data available for analysis. Please check data loading steps.")

if __name__ == "__main__":
    main()
