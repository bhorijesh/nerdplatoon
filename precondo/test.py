import requests
from base.api_config import *
import time
from time import sleep
from bs4 import BeautifulSoup
import pandas as pd
from base.log_config import logger
import json
import html


def fetch_data_from_api(querystring, headers):
    """
    Fetch data from the API using dynamic querystring and headers.
    """
    try:
        # Validate querystring and headers
        if not querystring or not headers:
            logger.error("Querystring or headers are not properly configured.")
            return None

        # Send a GET request to the API
        response = requests.get(api_url, params=querystring, headers=headers)
        response.raise_for_status()  

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

def save_data_to_csv(data, country_name, filename="api_url.csv"):
    """
    Save the extracted data to a CSV file with 'link' extracted from 'html' column.
    Appends country-specific data to the CSV file.
    """
    try:
        if 'map' in data:
            df = pd.DataFrame(data['map'])
            if 'html' in df.columns:
                df['link'] = df['html'].apply(lambda x: extract_href_from_html(x))
                df.drop(columns=['html'], inplace=True)

            if 'zoom' in df.columns:
                df.drop(columns=['zoom'], inplace=True)
            if 'title' in df.columns:
                df["title"] = df['title'].apply(html.unescape)

            # Add country name to the DataFrame
            df['country'] = "Canada"
            # Append to the CSV file
            with open(filename, 'a') as f:
                df.to_csv(f, index=False, header=f.tell() == 0)  
            logger.info(f"Data for {country_name} saved to {filename}")
        else:
            logger.error(f"No 'map' key found in the response for {country_name}. Check the API structure.")
    except Exception as e:
        logger.error(f"Error saving data to CSV for {country_name}: {e}")

def main():
    """
    Main function to fetch data for all countries and save it to a file.
    """
    for country_name, in country_listing.items():
        logger.info(f"Processing data for {country_name}...")

        # Get dynamic querystring and headers
        querystring, headers = get_dynamic_query_and_headers(country_name)

        # Fetch data from the API
        data = fetch_data_from_api(querystring, headers)

        if data:
            # Save the data to a CSV file
            save_data_to_csv(data, country_name)

if __name__ == "__main__":
    main()
