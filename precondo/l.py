import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from base.log_config import logger

def fetch_data_from_api():
    """
    Fetch data from the API with the specified query parameters.
    """
    # API endpoint
    api_url = "https://precondo.ca/wp-admin/admin-ajax.php"  # Replace with the actual API endpoint

    # Query parameters
    querystring = {"action": "list_listing", "filter[]": "843", "term": "163", "r": "0.18986753143991475", "test": "1"}

    payload = ""
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": "https://precondo.ca/",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }

    try:
        # Send a GET request to the API
        response = requests.get(api_url, data=payload, params=querystring, headers=headers)
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx

        # Parse the JSON response
        data = response.json()
        logger.info("Data fetched successfully.")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None

def extract_href_from_html(html_content):
    """
    Extract the href attribute from the given HTML content.
    """
    try:
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            a_tag = soup.find('a') 
            if a_tag and 'href' in a_tag.attrs:
                return a_tag['href']  
        return None 
    except Exception as e:
        logger.error(f"Error extracting href: {e}")
        return None

def save_data_to_csv(data, filename="api_url.csv"):
    """
    Save the extracted data to a CSV file with 'link' extracted from 'html' column.
    """
    try:
        if 'map' in data:
            df = pd.DataFrame(data['map'])
            if 'html' in df.columns:
                df['link'] = df['html'].apply(lambda x: extract_href_from_html(x))
                df.drop(columns=['html'], inplace=True)  
            df.drop(columns=['zoom'],inplace=True)

            # Save the DataFrame to a CSV file
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
        else:
            logger.error("No 'map' key found in the response. Check the API structure.")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")

def main():
    """
    Main function to fetch data from the API and save it to a file.
    """
    data = fetch_data_from_api()

    if data:
        # Save the data to a CSV file
        save_data_to_csv(data)

if __name__ == "__main__":
    main()
