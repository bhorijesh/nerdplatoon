import concurrent.futures
import pandas as pd
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine
from details import scrape_data  # Make sure this function is defined in details.py
import pymysql
import requests

# Define constants
QUERYSTRING = {
    "ajaxcall": "true",
    "ajaxtarget": "privatenotes,listingmeta,customlistinginfo_attributiontext,listingdetailsmap,routeplanner,listingcommunityinfo,metainformation,listingmeta_bottom,listingmedia,listingpropertydescription,listingtabtitles,listingtools_save_bottom,customlistinginfo_commentsshort,listingtools,listingtools_mobile,listinginfo,listingmarkettrendsmodule,localguidelistingdetailspage,listingdrivetime,listingphotos,listingdetails",
    "cms_current_mri": "119274"
}
HEADERS = {
    "accept": "application/xml, text/xml, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}
DB_TABLE_NAME = "Bahamas"  # Define your table name here

class PropertyUpdater:
    def __init__(self):
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

    def fetch_link(self, i, link: str) -> dict:
        url = f"{link}/xml-out"
        response = requests.get(url, headers=HEADERS, params=QUERYSTRING)

        if response.status_code == 200 and response.content.strip():
            print(f"Data fetched successfully from {url}")
            detailed_data = self.detail_extractor(link)
            
            # Initialize base data
            extracted_data = {
                'price': None, 'img': None, 'link': link,
                'updated_at': datetime.date.today().strftime('%Y/%m/%d'),
                'status_404': 0, 'about': None, 'bedroom': None,
                'full_baths': None, 'partial_bath': None,
                'Property_type': None, 'amenities': None,
                'exterior_details': None, 'Property_Details': None,
                'Interior_details': None, 'new_feature': None,
                'interior': None, 'exterior': None
            }

            # Update extracted_data only if detailed_data is not empty
            if detailed_data:
                extracted_data.update(detailed_data)

            return extracted_data
        else:
            print(f"Failed to fetch data from {url} - Status code: {response.status_code}")
            return {'status_404': 1, 'link': link}

    def validating_with_db_data(self, db_data: pd.DataFrame, extracted_data: dict) -> tuple[dict | None, bool]:
        VALUE_CHANGED = False
        IMAGES_CHANGED = False
        changed_extracted_data = {}
        db_row = db_data[db_data['link'] == extracted_data['link']]

        if not db_row.empty:
            changed_row = {}
            for key in extracted_data.keys():
                if key in db_row.columns:
                    db_value = db_row[key].iloc[0]
                    extracted_value = extracted_data[key]

                    if db_value == '':
                        db_value = None
                    if key == 'img':
                        db_value_copy = db_value.split(',') if db_value else []
                        if len(extracted_value) != len(db_value_copy):
                            IMAGES_CHANGED = True
                            print(f"{extracted_data['link']} :: {key} CHANGED FROM {len(db_value_copy)} TO {len(extracted_value)}")
                            changed_row[key] = extracted_value
                    elif key == 'new_feature':
                        if extracted_value != db_value:
                            VALUE_CHANGED = True
                            changed_row[key] = extracted_value
                            print(f"{extracted_data['link']} :: {key} CHANGED FROM {db_value} TO {extracted_value}")
                    else:
                        if str(extracted_value) != str(db_value):
                            VALUE_CHANGED = True
                            changed_row[key] = extracted_value
                            print(f"{extracted_data['link']} :: {key} CHANGED FROM {db_value} TO {extracted_value}")

            if VALUE_CHANGED or IMAGES_CHANGED:
                changed_row.update({
                    'id': db_row['id'].iloc[0],
                    'link': extracted_data['link'],
                    'updated_at': datetime.date.today().strftime('%Y/%m/%d')
                })
                changed_extracted_data.update(changed_row)
        else:
            print(f"Link not found in database: {extracted_data['link']}")
            VALUE_CHANGED = True
            changed_extracted_data.update(extracted_data)

        if VALUE_CHANGED or IMAGES_CHANGED:
            return changed_extracted_data, IMAGES_CHANGED
        else:
            return None, IMAGES_CHANGED

    def delete_not_found_items(self, filtered_df: dict) -> None:
        query_tuple = (datetime.date.today().strftime('%Y/%m/%d'), datetime.date.today().strftime('%Y/%m/%d'), filtered_df['link'])
        try:
            self.db_connection.ping(reconnect=True)
            self.cursor.execute(f"UPDATE {DB_TABLE_NAME} SET deleted_at = %s, updated_at = %s WHERE link = %s", query_tuple)
            self.db_connection.commit()
            print(f"Successfully deleted data for link: {filtered_df['link']}")
        except Exception as e:
            self.db_connection.rollback()
            print(f"Error deleting not found items: {str(e)}")
            raise

    def update_data(self, changed_data: dict) -> None:
        update_fields = [f"{key} = %s" for key in changed_data.keys() if key not in ['id', 'link']]
        update_query = f"UPDATE {DB_TABLE_NAME} SET {', '.join(update_fields)} WHERE link = %s"
        
        update_values = [changed_data[key] for key in changed_data.keys() if key not in ['id', 'link']]
        update_values.append(changed_data['link'])
        try:
            self.db_connection.ping(reconnect=True)
            self.cursor.execute(update_query, tuple(update_values))
            self.db_connection.commit()
            print(f"Successfully updated data for link: {changed_data['link']}")
        except Exception as e:
            self.db_connection.rollback()
            print(f"Error updating data: {str(e)}")

    def get_database_links(self) -> pd.DataFrame:
        query = "SELECT link FROM Bahamas WHERE deleted_at IS NULL"
        try:
            df = pd.read_sql(query, con=self.engine)
            print(f"Fetched {len(df)} links from the database.")
            return df
        except Exception as e:
            print(f"Error fetching links from the database: {e}")
            return pd.DataFrame()

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
            
            # Initialize a dictionary for the listing details
            listing_details = {}
            
            try:
                soup = BeautifulSoup(extraction.get('html', ''), 'html.parser')
                list_info = soup.find_all('dl', class_="listing-info__box-content")
                for item in list_info:
                    key_element = item.find('dt', class_="listing-info__title")
                    value_element = item.find('dd', class_="listing-info__value")
                    key = key_element.text.strip() if key_element else None
                    value = value_element.text.strip() if value_element else None
                    if key in ["Full Bath", "Full Baths"]:
                        listing_details["full_baths"] = value
                    elif key in ["Partial Bath", "Partial Baths"]:
                        listing_details["partial_bath"] = value
                    elif key in ["Bedrooms", "Bedroom"]:
                        listing_details["bedroom"] = value
                    elif key and value:
                        listing_details[key] = value
            except Exception as e:
                print(f"Error extracting listing info: {e}")

            amenities = None  
            try:
                soup = BeautifulSoup(extraction.get('html', ''), 'html.parser')
                amenities_containers = soup.find_all('div', class_='prop-description__amenities')
                if amenities_containers:
                    amenities = []  
                    for container in amenities_containers:
                        list_amenities = container.find('ul', class_="grid grid--spaced-all")
                        if list_amenities:
                            for li in list_amenities.find_all('li'):
                                if li:
                                    amenities.append(li.text.strip()) 
            except Exception as e:
                print(f"Error: {e}")
            
            property_details = {}
            if amenities is None:
                amenities = []
            
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
                
            return {
                'price': price, 'img': img_urls, 'link': link,
                'updated_at': datetime.date.today().strftime('%Y/%m/%d'),
                'status_404': 0, 'about': about, 'bedroom': None,
                'full_baths': None, 'partial_bath': None,
                'Property_type': property_type, 'amenities': amenities,
                'exterior_details': None, 'Property_Details': listing_details.get('property_details', None),
                'Interior_details': None, 'new_feature': new_feature,
                'interior': None, 'exterior': None
            }
        except Exception as e:
            print(f'data extraction failed: {e}')

    def driver_initialization(self):
        """Initializes and returns a Selenium WebDriver."""
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(options=options)
        return driver

    def main(self):
        df = self.get_database_links()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i, link in enumerate(df['link']):
                # Pass both index and link to fetch_link
                futures.append(executor.submit(self.fetch_link, i, link))

            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                if data.get('status_404') == 1:
                    self.delete_not_found_items(data)
                else:
                    changed_data, _ = self.validating_with_db_data(df, data)
                    if changed_data:
                        self.update_data(changed_data)

        self.driver.quit()

if __name__ == "__main__":
    updater = PropertyUpdater()
    updater.main()
