import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional, List

class Plotter:
    """
    A class for generating various visualizations of train delay and weather data.
    """

    def __init__(self):
        """
        Initializes the Plotter.
        """
        sns.set_theme(style="whitegrid")

    def plot_delay_distribution(self, df: pd.DataFrame, column: str = 'delay_minutes', 
                                title: str = 'Distribution of Train Delay Minutes',
                                 bins: int = 30, figsize: tuple = (10, 6)) -> None:
        """
        Plots the distribution of a numerical column (e.g., train delay minutes).
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            column (str): The numerical column to plot.
            title (str): Title of the plot.
            bins (int): Number of bins for the histogram.
            figsize (tuple): Figure size.
        """
        if column not in df.columns or not pd.api.types.is_numeric_dtype(df[column]):
            print(f"Error: Column '{column}' not found or is not numeric. Cannot plot distribution.")
            return

        plt.figure(figsize=figsize)
        sns.histplot(df[column].dropna(), bins=bins, kde=True)
        plt.title(title)
        plt.xlabel("Delay Minutes")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.show()

    def plot_time_series(self, df: pd.DataFrame, time_col: str, value_col: str, 
                         title: str = 'Time Series of Average Train Delays',
                         ylabel: str = 'Average Delay Minutes', figsize: tuple = (12, 6)) -> None:
        """
        Plots a time series of a numerical column, typically aggregated data.
        
        Args:
            df (pd.DataFrame): The input DataFrame (should ideally be pre-aggregated).
            time_col (str): The datetime column.
            value_col (str): The numerical column to plot.
            title (str): Title of the plot.
            ylabel (str): Y-axis label.
            figsize (tuple): Figure size.
        """
        if time_col not in df.columns or value_col not in df.columns:
            print(f"Error: Required columns '{time_col}' or '{value_col}' not found. Cannot plot time series.")
            return
        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            print(f"Error: Time column '{time_col}' is not a datetime type. Cannot plot time series.")
            return

        plt.figure(figsize=figsize)
        sns.lineplot(data=df, x=time_col, y=value_col)
        plt.title(title)
        plt.xlabel("Time")
        plt.ylabel(ylabel)
        plt.tight_layout()
        plt.show()

    def plot_correlation_heatmap(self, df: pd.DataFrame, columns: Optional[List[str]] = None,
                                 title: str = 'Correlation Heatmap', figsize: tuple = (10, 8)) -> None:
        """
        Plots a correlation heatmap for selected numerical columns.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            columns (Optional[List[str]]): List of numerical columns to include in the heatmap.
                                          If None, uses all numerical columns.
            title (str): Title of the plot.
            figsize (tuple): Figure size.
        """
        if columns:
            numeric_df = df[columns].select_dtypes(include=np.number)
        else:
            numeric_df = df.select_dtypes(include=np.number)

        if numeric_df.empty:
            print("No numeric columns to plot for correlation heatmap.")
            return

        plt.figure(figsize=figsize)
        sns.heatmap(numeric_df.corr(), annot=True, cmap='coolwarm', fmt=".2f")
        plt.title(title)
        plt.tight_layout()
        plt.show()

    def plot_category_vs_delay(self, df: pd.DataFrame, category_col: str, value_col: str = 'delay_minutes',
                               title: str = 'Average Delay by Category', 
                               xlabel: str = 'Category', ylabel: str = 'Average Delay Minutes',
                               figsize: tuple = (10, 6)) -> None:
        """
        Plots the average delay for different categories using a bar plot.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            category_col (str): The categorical column.
            value_col (str): The numerical column (e.g., delay minutes) to average.
            title (str): Title of the plot.
            xlabel (str): X-axis label.
            ylabel (str): Y-axis label.
            figsize (tuple): Figure size.
        """
        if category_col not in df.columns or value_col not in df.columns:
            print(f"Error: Required columns '{category_col}' or '{value_col}' not found. Cannot plot category vs. delay.")
            return
        if not pd.api.types.is_numeric_dtype(df[value_col]):
            print(f"Error: Value column '{value_col}' is not numeric. Cannot plot category vs. delay.")
            return

        avg_delay = df.groupby(category_col)[value_col].mean().reset_index()
        
        plt.figure(figsize=figsize)
        sns.barplot(data=avg_delay, x=category_col, y=value_col)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    # Example usage with dummy data
    plotter = Plotter()
    
    # Create a dummy merged and analyzed DataFrame
    dummy_data = {
        "scheduled_time": pd.to_datetime(["2023-01-15 08:00", "2023-01-15 10:00", "2023-01-16 08:00", "2023-01-16 10:00", "2023-01-17 08:00", "2023-01-17 12:00"]),
        "delay_minutes": [5, 0, 10, 5, 0, 15],
        "temperature": [2.5, 3.0, -1.0, 0.0, 5.0, 6.0],
        "humidity": [85, 80, 90, 88, 70, 75],
        "weather_condition": ["cloudy", "sunny", "snowy", "snowy", "rainy", "rainy"],
        "route": ["Ostrava-Frenstat", "Ostrava-Frydlant", "Ostrava-Frenstat", "Ostrava-Frydlant", "Ostrava-Frenstat", "Ostrava-Frenstat"]
    }
    df_analyzed = pd.DataFrame(dummy_data)

    print("--- Plotting Delay Distribution ---")
    plotter.plot_delay_distribution(df_analyzed, 'delay_minutes')

    print("\n--- Plotting Time Series of Average Delays (Daily) ---")
    # Aggregate data daily for time series plot
    daily_avg_delay = df_analyzed.set_index('scheduled_time')['delay_minutes'].resample('D').mean().reset_index()
    plotter.plot_time_series(daily_avg_delay, 'scheduled_time', 'delay_minutes', 
                             title='Daily Average Train Delay Minutes')

    print("\n--- Plotting Correlation Heatmap ---")
    plotter.plot_correlation_heatmap(df_analyzed, columns=['delay_minutes', 'temperature', 'humidity'])

    print("\n--- Plotting Average Delay by Weather Condition ---")
    plotter.plot_category_vs_delay(df_analyzed, 'weather_condition', title='Average Delay by Weather Condition')
    
    print("\n--- Plotting Average Delay by Route ---")
    plotter.plot_category_vs_delay(df_analyzed, 'route', title='Average Delay by Route')
