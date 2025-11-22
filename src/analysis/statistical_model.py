import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, Any, Tuple, List, Union

class StatisticalAnalyzer:
    """
    A class for performing statistical analysis on train delay data.
    """

    def __init__(self):
        """
        Initializes the StatisticalAnalyzer.
        """
        pass

    def get_descriptive_statistics(self, df: pd.DataFrame, column: str) -> Optional[pd.Series]:
        """
        Calculates descriptive statistics for a specified numerical column.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            column (str): The name of the numerical column.

        Returns:
            Optional[pd.Series]: A Series containing descriptive statistics, or None if column not found.
        """
        if column not in df.columns or not pd.api.types.is_numeric_dtype(df[column]):
            print(f"Error: Column '{column}' not found or is not numeric.")
            return None
        
        try:
            return df[column].describe()
        except Exception as e:
            print(f"An error occurred while calculating descriptive statistics: {e}")
            return None

    def perform_t_test(self, df: pd.DataFrame, group_col: str, value_col: str, 
                       group1_val: Any, group2_val: Any) -> Optional[Tuple[float, float]]:
        """
        Performs an independent samples t-test between two groups for a specified value column.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            group_col (str): The column used to define the groups.
            value_col (str): The numerical column for which to compare means.
            group1_val (Any): The value defining the first group.
            group2_val (Any): The value defining the second group.

        Returns:
            Optional[Tuple[float, float]]: A tuple containing the t-statistic and p-value,
                                            or None if an error occurs.
        """
        if group_col not in df.columns or value_col not in df.columns:
            print(f"Error: Required columns '{group_col}' or '{value_col}' not found.")
            return None
        if not pd.api.types.is_numeric_dtype(df[value_col]):
            print(f"Error: Value column '{value_col}' is not numeric.")
            return None

        try:
            group1 = df[df[group_col] == group1_val][value_col].dropna()
            group2 = df[df[group_col] == group2_val][value_col].dropna()

            if len(group1) < 2 or len(group2) < 2:
                print("Error: Not enough data points in one or both groups for t-test.")
                return None
            
            t_statistic, p_value = stats.ttest_ind(group1, group2)
            return t_statistic, p_value
        except Exception as e:
            print(f"An error occurred during t-test: {e}")
            return None

    def calculate_correlation(self, df: pd.DataFrame, col1: str, col2: str, 
                              method: str = 'pearson') -> Optional[float]:
        """
        Calculates the correlation between two numerical columns.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            col1 (str): Name of the first numerical column.
            col2 (str): Name of the second numerical column.
            method (str): Correlation method ('pearson', 'kendall', 'spearman').

        Returns:
            Optional[float]: The correlation coefficient, or None if an error occurs.
        """
        if col1 not in df.columns or col2 not in df.columns:
            print(f"Error: Columns '{col1}' or '{col2}' not found.")
            return None
        if not pd.api.types.is_numeric_dtype(df[col1]) or not pd.api.types.is_numeric_dtype(df[col2]):
            print(f"Error: One or both columns are not numeric for correlation calculation.")
            return None

        try:
            return df[col1].corr(df[col2], method=method)
        except Exception as e:
            print(f"An error occurred during correlation calculation: {e}")
            return None

    def aggregate_by_time(self, df: pd.DataFrame, time_col: str, value_col: str, 
                          freq: str = 'H', agg_func: Union[str, Dict] = 'mean') -> pd.DataFrame:
        """
        Aggregates a numerical column by a time-based column.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            time_col (str): The name of the datetime column.
            value_col (str): The numerical column to aggregate.
            freq (str): The frequency for resampling (e.g., 'H' for hourly, 'D' for daily).
            agg_func (Union[str, Dict]): Aggregation function (e.g., 'mean', 'sum', or a dict for multiple functions).

        Returns:
            pd.DataFrame: A DataFrame with aggregated data.
        """
        if time_col not in df.columns or value_col not in df.columns:
            print(f"Error: Required columns '{time_col}' or '{value_col}' not found for aggregation.")
            return pd.DataFrame()
        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            print(f"Error: Time column '{time_col}' is not a datetime type.")
            return pd.DataFrame()
        if not pd.api.types.is_numeric_dtype(df[value_col]):
            print(f"Error: Value column '{value_col}' is not numeric.")
            return pd.DataFrame()

        try:
            df_indexed = df.set_index(time_col)
            if isinstance(agg_func, str):
                return df_indexed[value_col].resample(freq).agg(agg_func).to_frame()
            elif isinstance(agg_func, dict):
                return df_indexed[value_col].resample(freq).agg(agg_func)
            else:
                print("Error: agg_func must be a string or a dictionary.")
                return pd.DataFrame()
        except Exception as e:
            print(f"An error occurred during time-based aggregation: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Example usage with dummy data
    analyzer = StatisticalAnalyzer()
    
    # Create a dummy merged DataFrame (similar to what cleaner.py would output)
    dummy_data: List[Dict[str, Any]] = [
        {"train_id": "R123", "scheduled_time": "2023-01-15 08:00:00", "actual_time": "2023-01-15 08:05:00", "delay_minutes": 5, "route": "Ostrava-Frenstat", "date": "2023-01-15", "temperature": 2.5, "weather_condition": "cloudy"},
        {"train_id": "EC456", "scheduled_time": "2023-01-15 10:30:00", "actual_time": "2023-01-15 10:30:00", "delay_minutes": 0, "route": "Ostrava-Frydlant", "date": "2023-01-15", "temperature": 3.0, "weather_condition": "sunny"},
        {"train_id": "R123", "scheduled_time": "2023-01-16 08:00:00", "actual_time": "2023-01-16 08:10:00", "delay_minutes": 10, "route": "Ostrava-Frenstat", "date": "2023-01-16", "temperature": -1.0, "weather_condition": "snowy"},
        {"train_id": "EC456", "scheduled_time": "2023-01-16 10:30:00", "actual_time": "2023-01-16 10:35:00", "delay_minutes": 5, "route": "Ostrava-Frydlant", "date": "2023-01-16", "temperature": 0.0, "weather_condition": "snowy"},
        {"train_id": "R123", "scheduled_time": "2023-01-17 08:00:00", "actual_time": "2023-01-17 08:00:00", "delay_minutes": 0, "route": "Ostrava-Frenstat", "date": "2023-01-17", "temperature": 5.0, "weather_condition": "rainy"},
        {"train_id": "R123", "scheduled_time": "2023-01-17 12:00:00", "actual_time": "2023-01-17 12:15:00", "delay_minutes": 15, "route": "Ostrava-Frenstat", "date": "2023-01-17", "temperature": 6.0, "weather_condition": "rainy"},
    ]
    
    df_merged = pd.DataFrame(dummy_data)
    df_merged['scheduled_time'] = pd.to_datetime(df_merged['scheduled_time'])
    df_merged['date'] = pd.to_datetime(df_merged['date'])

    print("--- Descriptive Statistics for Delay Minutes ---")
    delay_stats = analyzer.get_descriptive_statistics(df_merged, 'delay_minutes')
    if delay_stats is not None:
        print(delay_stats)

    print("\n--- T-test for Delay Minutes: Snowy vs. Not Snowy Conditions ---")
    # For demonstration, let's create a 'is_snowy' column
    df_merged['is_snowy'] = df_merged['weather_condition'].apply(lambda x: 1 if x == 'snowy' else 0)
    t_test_results = analyzer.perform_t_test(df_merged, 'is_snowy', 'delay_minutes', 1, 0)
    if t_test_results:
        t_stat, p_val = t_test_results
        print(f"T-statistic: {t_stat:.2f}, P-value: {p_val:.3f}")
        if p_val < 0.05:
            print("Conclusion: Significant difference in delay minutes between snowy and non-snowy conditions.")
        else:
            print("Conclusion: No significant difference in delay minutes between snowy and non-snowy conditions.")

    print("\n--- Correlation between Delay Minutes and Temperature ---")
    correlation = analyzer.calculate_correlation(df_merged, 'delay_minutes', 'temperature')
    if correlation is not None:
        print(f"Pearson Correlation: {correlation:.2f}")

    print("\n--- Hourly Average Delay Minutes ---")
    hourly_avg_delay = analyzer.aggregate_by_time(df_merged, 'scheduled_time', 'delay_minutes', freq='H', agg_func='mean')
    print(hourly_avg_delay)
    
    # Save processed data for further use
    # In a real scenario, you'd save the results of your analysis, not just the raw merged data
    # For now, let's just show that analysis is done on merged data.
    # df_merged.to_csv("../../data/processed/analyzed_delays_weather.csv", index=False)
    # print("\nAnalyzed data (merged) saved to data/processed/analyzed_delays_weather.csv")
