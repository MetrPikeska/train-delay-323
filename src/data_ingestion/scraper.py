import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Any, Optional

def scrape_idos_delays(url: str, params: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Scrapes train delay data from IDOS/GRAPP.
    
    Args:
        url (str): The URL of the IDOS/GRAPP page to scrape.
        params (Optional[Dict[str, str]]): Optional parameters for the GET request.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                              represents a train delay record.
    """
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # This is a placeholder for actual scraping logic.
        # The exact CSS selectors/HTML structure will depend on the IDOS/GRAPP website.
        # You'll need to inspect the target website to get the correct selectors.
        
        # Example: Find all tables and extract rows
        data: List[Dict[str, Any]] = []
        
        # Placeholder for extracting data from a hypothetical table
        # For a real scenario, you'd target specific tables, rows, and columns
        # based on the website's structure.
        
        # For demonstration purposes, let's assume a table with specific headers
        table = soup.find('table', class_='delays-table')
        if table:
            headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
            for row in table.find('tbody').find_all('tr'):
                values = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(headers) == len(values):
                    data.append(dict(zip(headers, values)))
        else:
            print("No table with class 'delays-table' found. Adjust selectors as needed.")

        return data
    except requests.exceptions.RequestException as e:
        print(f"Error during web scraping: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

if __name__ == "__main__":
    # Example usage:
    # Replace with the actual URL and parameters for IDOS/GRAPP
    idos_url = "https://www.idos.cz/vlaky/spojeni/" # This is a placeholder URL
    
    # You would typically pass parameters like date, origin, destination
    # to filter the results.
    search_params = {
        "f": "Ostrava",
        "t": "Frenštát",
        "date": "22.11.2025",
        "time": "08:00"
    }

    print(f"Scraping data from: {idos_url} with params: {search_params}")
    delay_data = scrape_idos_delays(idos_url, search_params)

    if delay_data:
        df = pd.DataFrame(delay_data)
        print("Scraped Data Head:")
        print(df.head())
        df.to_csv("../../data/raw/idos_delays_raw.csv", index=False)
        print("Scraped data saved to data/raw/idos_delays_raw.csv")
    else:
        print("No data scraped.")
