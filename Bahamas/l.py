import re
import pandas as pd
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine
from details import scrape_data
import pymysql
import requests

class PropertyScraper:
    def __init__(self):
        # Initialize database connections
        self.db_connection, self.cursor = self.connect_database_with_pymysql()
        self.engine = self.connect_database_with_sqlalchemy()
        self.driver = self.driver_initialization()

    def connect_database_with_sqlalchemy(self):
        try:
            db_url = 'mysql+pymysql://root:new_password@localhost:3306/practice'
            engine = create_engine(db_url)
            engine.connect()
            print('Database connected successfully')
            return engine
        except Exception as e:
            print(f'Error while connecting to the database: {e}')
            return None

    def connect_database_with_pymysql(self):
        try:
            connection = pymysql.connect(
                host='localhost',
                user='root',
                password='new_password',
                db='practice'
            )
            cursor = connection.cursor()
            print("MySQL connection established")
            return connection, cursor
        except Exception as e:
            print(f'Error while connecting to MySQL: {e}')
            return None, None

    def detail_extractor(self, link):
        """Extract details for a given property link using the scrape_data function."""
        try:
            # Scrape the data using the scrape_data function
            extraction = scrape_data(self.driver, link)
            
            # Extract the necessary data from the scraped information
            price = extraction.get('price', None)
            title = extraction.get('title', None)
            address = extraction.get('location', None)
            about = extraction.get('about', None)
            amenities = extraction.get('amenities', [])
            img_urls = extraction.get('img', [])
            property_type = extraction.get('type', None)
            new_feature = extraction.get('new_feature', None)
            latitude = extraction.get('lat', None)
            longitude = extraction.get('lng', None)
            unique_id = extraction.get('uuid', None)
            created_at = extraction.get('created_at', None)
            
            # Initialize a dictionary for the listing details
            listing_details = {}
            
            # Parse the listing_info to extract additional details
            try:
                soup = BeautifulSoup(extraction['listing_info'], 'html.parser')
                list_info = soup.find_all('dl', class_="listing-info__box-content")
                for item in list_info:
                    key_element = item.find('dt', class_="listing-info__title")
                    value_element = item.find('dd', class_="listing-info__value")
                    key = key_element.text.strip() if key_element else None
                    value = value_element.text.strip() if value_element else None
                    if key and value:
                        if key in ["Full Bath", "Full Baths"]:
                            listing_details["full_baths"] = value
                        elif key in ["Partial Bath", "Partial Baths"]:
                            listing_details["partial_bath"] = value
                        elif key in ["Bedrooms", "Bedroom"]:
                            listing_details["bedroom"] = value
                        else:
                            listing_details[key] = value
            except Exception as e:
                print(f"Error extracting listing info: {e}")
            
            # Extract amenities if available
            try:
                amenities_containers = soup.find_all('div', class_='prop-description__amenities')
                if amenities_containers:
                    for container in amenities_containers:
                        list_amenities = container.find('ul', class_="grid grid--spaced-all")
                        if list_amenities:
                            for li in list_amenities.find_all('li'):
                                if li:
                                    amenities.append(li.text.strip())
            except Exception as e:
                print(f"Error extracting amenities: {e}")
            
            # Scrape the property details (size, year built, etc.)
            property_details = {}
            if amenities is None:
                amenities = []  # Ensure amenities is always a list
            try:
                details_container = soup.find('div', class_='prop-description__details grid grid--spaced-all')
                if details_container:
                    for section in details_container.find_all('h3', class_='prop-description__title'):
                        section_key = section.text.strip()
                        if section_key == "Exterior":
                            section_key = "exterior_details"
                        elif section_key == "Interior":
                            section_key = "interior_details"
                        
                        section_values = {}
                        next_sibling = section.find_next_sibling('dl')
                        if next_sibling:
                            dt_elements = next_sibling.find_all('dt', class_="prop-description__label")
                            dd_elements = next_sibling.find_all('dd', class_="prop-description__value")
                            for dt, dd in zip(dt_elements, dd_elements):
                                detail_key = dt.text.strip()
                                detail_value = dd.text.strip()
                                if detail_key == 'Amenities':
                                    amenities.append(detail_value)
                                elif detail_key and detail_value:
                                    section_values[detail_key] = detail_value
                        if section_values:
                            property_details[section_key] = section_values
            except Exception as e:
                print(f"Error extracting property details: {e}")
            
            # Scrape the features details section with dt and dd as key-value pairs
            features_details = {}
            try:
                feature_container = soup.find('div', class_='prop-description__features u-margin-bottom')
                if feature_container:
                    dl_elements = feature_container.find_all('dl', class_="u-margin-bottom")
                    for dl_element in dl_elements:
                        dt_elements = dl_element.find_all('dt', class_="prop-description__label")
                        dd_elements = dl_element.find_all('dd', class_="prop-description__value")
                        for dt, dd in zip(dt_elements, dd_elements):
                            detail_key = dt.get_text(strip=True)
                            detail_value = dd.get_text(strip=True)
                            if detail_key and detail_value:
                                features_details[detail_key] = detail_value
            except Exception as e:
                print(f"Error extracting features details: {e}")
            
            return {
                'title': title,
                'price': price,
                'address': address,
                'about': about,
                'amenities': amenities,
                'img_urls': img_urls,
                'type': property_type,
                'new_feature': new_feature,
                'latitude': latitude,
                'longitude': longitude,
                'uuid': unique_id,
                'created_at': created_at,
                **listing_details,
                **property_details,
                **features_details
            }
        
        except Exception as e:
            print(f"Error extracting details from {link}: {e}")
            return None
    def driver_initialization(self):
        # Initialize and return the Selenium WebDriver
        driver = webdriver.Chrome()
        return driver

    def fetch_link(self, link, i):
        """Fetch data for each property link, checking HTTP status codes first."""
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
            response = requests.get(link)
            status_code = response.status_code

            # If the status code is 200 (OK), proceed with scraping
            if status_code == 200:
                print(f"Status 200: OK - Scraping the page {link} (Link {i})")
                self.driver.get(link)  # Use Selenium to get the content
                details = self.detail_extractor(link)

                return details
            elif status_code == 400:
                print(f"Status 400: Bad Request - Skipping the page {link} (Link {i})")
                return None

            else:
                print(f"Unexpected status code {status_code} for link {link}. Skipping.")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed for link {link}: {e}")
            return None


    def save_to_database(self, data):
        """Function to save the scraped data into the database."""
        try:
            df = pd.DataFrame([data])
            df.to_sql('scraped_properties', con=self.engine, if_exists='append', index=False)
            print("Data saved to the database.")
        except Exception as e:
            print(f"Error saving data to the database: {e}")
    def get_database_links(self):
        """Retrieve links from the database for processing."""
        try:
            query = """
                SELECT id, title, Mls_Id, Web_Id, link, price, price_unit, img, about, 
                    Exterior, Interior, bedroom, full_baths, partial_bath, Property_type, 
                    amenities, exterior_details, Property_Details, interior_details, new_feature 
                FROM Bahamas WHERE deleted_at IS NULL
            """
            # Execute the query and store the result in a DataFrame
            df = pd.read_sql(query, con=self.engine.connect())
            return df
        except Exception as e:
            print(f"Error retrieving database links: {e}")
            return pd.DataFrame()

    def validating_with_db_data(self, db_data: pd.DataFrame, extracted_data: dict) -> tuple[dict | None, bool]:
        VALUE_CHANGED = False
        IMAGES_CHANGED = False
        changed_extracted_data = {}

        db_row = db_data[db_data['link'] == extracted_data['link']]

        if db_row.empty:
            # If link not found, mark it as changed
            print(f"Link not found in database: {extracted_data['link']}")
            VALUE_CHANGED = True
            changed_extracted_data.update(extracted_data)
        else:
            changed_row = {}

            for key, extracted_value in extracted_data.items():
                if key in db_row.columns:
                    db_value = db_row[key].iloc[0]

                    # Handle missing values and data comparison
                    if db_value == '':
                        db_value = None

                    if extracted_value != db_value:
                        VALUE_CHANGED = True
                        changed_row[key] = extracted_value
                        print(f"Change detected for {key} (Link {extracted_data['link']}): {db_value} -> {extracted_value}")

            if VALUE_CHANGED:
                changed_row['id'] = db_row['id'].iloc[0]
                changed_row['link'] = extracted_data['link']
                changed_row['updated_at'] = datetime.date.today().strftime('%Y/%m/%d')
                changed_extracted_data.update(changed_row)

        return (changed_extracted_data, IMAGES_CHANGED) if VALUE_CHANGED or IMAGES_CHANGED else (None, False)

    def main(self):
        """Main function to start the scraping process."""
        if not self.engine:
            print("Database connection failed. Exiting...")
            return
        
        # Get the links from the database
        db_data = self.get_database_links()
        print(db_data.columns)  # Print column names to verify the structure
        
        if 'link' not in db_data.columns:
            print("Error: 'link' column is missing from the database data.")
            return
        
        links = db_data['link'].tolist()

        if links:
            for i, link in enumerate(links):
                try:
                    details = self.fetch_link(link, i)
                    if details:
                        self.save_to_database(details)  # Save the fetched data to the database
                except Exception as e:
                    print(f'Error while processing the link {link}: {e}')
            
            self.driver.quit()  # Close the Selenium driver after processing all links


if __name__ == '__main__':
    scraper = PropertyScraper()
    scraper.main()
