import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from typing import Optional, List, Dict, Any

class SpatialProcessor:
    """
    A class for processing geographical data, creating GeoDataFrames,
    and generating spatial outputs like shapefiles.
    """

    def __init__(self):
        """
        Initializes the SpatialProcessor.
        """
        pass

    def create_gdf_from_points(self, df: pd.DataFrame, lat_col: str, lon_col: str, 
                               crs: str = "EPSG:4326") -> Optional[gpd.GeoDataFrame]:
        """
        Creates a GeoDataFrame from a DataFrame with latitude and longitude columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame.
            lat_col (str): Name of the latitude column.
            lon_col (str): Name of the longitude column.
            crs (str): Coordinate Reference System (e.g., "EPSG:4326" for WGS84).

        Returns:
            Optional[gpd.GeoDataFrame]: A GeoDataFrame, or None if an error occurs.
        """
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude column '{lat_col}' or longitude column '{lon_col}' not found.")
            return None
        
        try:
            # Drop rows with missing lat/lon to avoid geometry creation errors
            df_cleaned = df.dropna(subset=[lat_col, lon_col]).copy()
            
            geometry = [Point(xy) for xy in zip(df_cleaned[lon_col], df_cleaned[lat_col])]
            gdf = gpd.GeoDataFrame(df_cleaned, geometry=geometry, crs=crs)
            return gdf
        except Exception as e:
            print(f"An error occurred while creating GeoDataFrame from points: {e}")
            return None

    def load_railway_line_shapefile(self, path: str) -> Optional[gpd.GeoDataFrame]:
        """
        Loads a railway line shapefile into a GeoDataFrame.
        
        Args:
            path (str): Path to the shapefile (e.g., "data/external/railway_line.shp").

        Returns:
            Optional[gpd.GeoDataFrame]: A GeoDataFrame of the railway line, or None if an error occurs.
        """
        try:
            gdf = gpd.read_file(path)
            return gdf
        except Exception as e:
            print(f"Error loading shapefile from {path}: {e}")
            return None

    def spatial_join_data(self, points_gdf: gpd.GeoDataFrame, polygons_gdf: gpd.GeoDataFrame,
                          how: str = 'inner') -> Optional[gpd.GeoDataFrame]:
        """
        Performs a spatial join between point data (e.g., train stops) and polygon data (e.g., districts).
        
        Args:
            points_gdf (gpd.GeoDataFrame): GeoDataFrame with point geometries.
            polygons_gdf (gpd.GeoDataFrame): GeoDataFrame with polygon geometries.
            how (str): Type of join (e.g., 'inner', 'left').

        Returns:
            Optional[gpd.GeoDataFrame]: The spatially joined GeoDataFrame, or None if an error occurs.
        """
        try:
            # Ensure both GeoDataFrames have the same CRS before spatial join
            if points_gdf.crs != polygons_gdf.crs:
                print(f"Warning: CRS mismatch. Reprojecting points_gdf to {polygons_gdf.crs}")
                points_gdf = points_gdf.to_crs(polygons_gdf.crs)

            joined_gdf = gpd.sjoin(points_gdf, polygons_gdf, how=how, op='within')
            return joined_gdf
        except Exception as e:
            print(f"An error occurred during spatial join: {e}")
            return None

    def save_gdf_to_shapefile(self, gdf: gpd.GeoDataFrame, path: str, driver: str = 'ESRI Shapefile') -> bool:
        """
        Saves a GeoDataFrame to a shapefile.
        
        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame to save.
            path (str): The output path for the shapefile (e.g., "data/outputs/delays.shp").
            driver (str): The OGR format driver to use for saving (e.g., 'ESRI Shapefile').

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            gdf.to_file(path, driver=driver)
            print(f"GeoDataFrame successfully saved to {path}")
            return True
        except Exception as e:
            print(f"Error saving GeoDataFrame to shapefile: {e}")
            return False

    def create_leaflet_geojson(self, gdf: gpd.GeoDataFrame, path: str) -> bool:
        """
        Converts a GeoDataFrame to a GeoJSON file suitable for Leaflet maps.
        
        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame to convert.
            path (str): The output path for the GeoJSON file (e.g., "docs/leaflet_layers/delays.geojson").

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Ensure CRS is WGS84 (EPSG:4326) for web maps like Leaflet
            if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
                gdf = gdf.to_crs(epsg=4326)
            
            gdf.to_file(path, driver='GeoJSON')
            print(f"GeoDataFrame successfully converted and saved to GeoJSON at {path}")
            return True
        except Exception as e:
            print(f"Error creating Leaflet GeoJSON: {e}")
            return False

if __name__ == "__main__":
    # Example usage with dummy data
    spatial_processor = SpatialProcessor()
    
    # Dummy train station data with delays
    station_data: List[Dict[str, Any]] = [
        {"station_name": "Ostrava hl.n.", "latitude": 49.8465, "longitude": 18.2917, "avg_delay": 5},
        {"station_name": "Frydlant n.O.", "latitude": 49.6645, "longitude": 18.3582, "avg_delay": 8},
        {"station_name": "Celadna", "latitude": 49.5760, "longitude": 18.3615, "avg_delay": 3},
        {"station_name": "Frenstat p.R.", "latitude": 49.5601, "longitude": 18.2140, "avg_delay": 10},
        {"station_name": "Undefined", "latitude": None, "longitude": None, "avg_delay": 1}, # Missing coords
    ]
    df_stations = pd.DataFrame(station_data)

    # 1. Create GeoDataFrame from points
    print("--- Creating GeoDataFrame from points ---")
    gdf_stations = spatial_processor.create_gdf_from_points(df_stations, 'latitude', 'longitude')
    if gdf_stations is not None:
        print(gdf_stations.head())
        # Save to shapefile
        spatial_processor.save_gdf_to_shapefile(gdf_stations, "../../data/processed/train_stations.shp")
        # Create Leaflet GeoJSON
        spatial_processor.create_leaflet_geojson(gdf_stations, "../../docs/leaflet_layers/train_stations.geojson")
    
    # Dummy geographical area (e.g., district polygons)
    # For a real scenario, you'd load an actual shapefile for regions/districts
    # This is a simplified representation.
    from shapely.geometry import Polygon
    poly1 = Polygon([(18.0, 49.5), (18.5, 49.5), (18.5, 50.0), (18.0, 50.0)])
    poly2 = Polygon([(18.5, 49.5), (19.0, 49.5), (19.0, 50.0), (18.5, 50.0)])
    
    district_data = {
        'district_name': ['Moravian-Silesian Region West', 'Moravian-Silesian Region East'],
        'geometry': [poly1, poly2]
    }
    gdf_districts = gpd.GeoDataFrame(district_data, crs="EPSG:4326")
    
    # 2. Example: Load a railway line shapefile (placeholder)
    # Assuming you have a shapefile like 'data/external/railway_line_323.shp'
    # railway_line_gdf = spatial_processor.load_railway_line_shapefile("../../data/external/railway_line_323.shp")
    # if railway_line_gdf is not None:
    #     print("\nRailway Line GeoDataFrame:")
    #     print(railway_line_gdf.head())

    # 3. Perform spatial join (example: join stations to districts)
    if gdf_stations is not None:
        print("\n--- Performing Spatial Join (Stations to Districts) ---")
        joined_stations_districts = spatial_processor.spatial_join_data(gdf_stations, gdf_districts)
        if joined_stations_districts is not None:
            print(joined_stations_districts.head())
            spatial_processor.save_gdf_to_shapefile(joined_stations_districts, "../../data/processed/stations_in_districts.shp")
