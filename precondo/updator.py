import concurrent.futures
import pandas as pd
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine
from detail import scrape_data  # Ensure this function is defined in details.py
import pymysql
import requests
from datetime import date
import json

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

DB_TABLE_NAME = "Precondo"  # Define your table name here

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
        
        # Initialize base data structure for the extracted data
        extracted_data = {
            'price': None, 'img': None, 'link': link,
            'title':None, 'location':None,
            'updated_at': datetime.date.today().strftime('%Y/%m/%d'),
            'about': None, 'bedroom': None,
            'full_baths': None, 'partial_bath': None,
            'Property_type': None, 'amenities': None,
            'exterior_details': None, 'Property_Details': None,
            'Interior_details': None, 'new_feature':None,
            'interior': None, 'exterior': None
        }

        if response.status_code == 200 and response.content.strip():
            print(f"Data fetched successfully from {url}")
            # Assuming detail_extractor method returns a dict of extracted details
            detailed_data = self.detail_extractor(link)
            if detailed_data:
                extracted_data.update(detailed_data)
            status_404 = 0
        elif response.status_code == 205:
            # Status code 205 indicates success, but content is unavailable
            print(f"Content not available from {url} (Status Code 205).")
            status_404 = 1
            # No need to populate extracted_data in this case
        else:
            # Handle unexpected or error responses
            print(f"Failed to fetch data from {url} - Status code: {response.status_code}")
            status_404 = 0
            extracted_data = {'status_404': status_404, 'link': link}

        return extracted_data


    def validating_with_db_data(self, db_data: pd.DataFrame, extracted_data: dict) -> tuple[dict | None, bool]:
        VALUE_CHANGED = False
        IMAGES_CHANGED = False
        changed_extracted_data = {}
        print(f"Validating link: {extracted_data['link']}")  # Print when validation starts for each link
        
        db_row = db_data[db_data['link'] == extracted_data['link']]
        
        if not db_row.empty:
            print(f"Link {extracted_data['link']} found in database.")  # Debug print for link found in DB
            changed_row = {}
            
            for key in extracted_data.keys():
                if key in db_row.columns:
                    db_value = db_row[key].iloc[0]
                    extracted_value = extracted_data[key]

                    if db_value == '':
                        db_value = None
                    if key == 'img_src':
                        db_value_copy = db_value.split(',') if db_value else []
                        if len(extracted_value) != len(db_value_copy):
                            IMAGES_CHANGED = True
                            print(f"{extracted_data['link']} :: {key} CHANGED FROM {len(db_value_copy)} TO {len(extracted_value)}")
                            changed_row[key] = extracted_value
                    elif key == 'amenities':
                        if extracted_value != db_value:
                            VALUE_CHANGED = True
                            changed_row[key] = extracted_value
                            print(f"{extracted_data['link']} :: {key} CHANGED FROM {db_value} TO {extracted_value}")
                else:
                    if str(extracted_value) != str(db_value):
                        VALUE_CHANGED = True
                        changed_row[key] = extracted_value

            if VALUE_CHANGED:
                changed_row['id'] = db_row['id'].iloc[0]
                changed_row['link'] = extracted_data['link']
                changed_row['updated_at'] = date.today().strftime('%Y/%m/%d')
                changed_extracted_data.update(changed_row)
            
            if IMAGES_CHANGED:
                changed_row['id'] = db_row['id'].iloc[0]
                changed_row['link'] = extracted_data['link']
                changed_row['updated_at'] = date.today().strftime('%Y/%m/%d')
                changed_row['title'] = db_row['title'].iloc[0]
                changed_extracted_data.update(changed_row)
            
            # Debug print for validation result
            if VALUE_CHANGED or IMAGES_CHANGED:
                print(f"Link {extracted_data['link']} validated, changes detected.")
            else:
                print(f"Link {extracted_data['link']} validated, no changes detected.")
        
        else:
            print(f"Link {extracted_data['link']} NOT found in database, considered changed.")  # Debug print for not found link
            VALUE_CHANGED = True
            changed_extracted_data.update(extracted_data)
        
        if VALUE_CHANGED or IMAGES_CHANGED:
            return changed_extracted_data, IMAGES_CHANGED
        else:
            print(f"Link {extracted_data['link']} NOT validated (no changes and not found in DB).")  # Debug print for not validated
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
        # Ensure that the 'img' field is properly serialized if it's a list
        if 'img_src' in changed_data and isinstance(changed_data['img_src'], list):
            changed_data['img_src'] = json.dumps(changed_data['img_src'])
        if 'amenities' in changed_data and isinstance(changed_data['amenities'], dict):
            changed_data['amenities'] = json.dumps(changed_data['amenities'])
        # Prepare the update query fields, excluding 'id' and 'link'
        update_fields = [f"{key} = %s" for key in changed_data.keys() if key not in ['id', 'link']]
        if not update_fields:
            return
        update_query = f"UPDATE {DB_TABLE_NAME} SET {', '.join(update_fields)} WHERE link = %s"

        update_values = [changed_data[key] for key in changed_data.keys() if key not in ['id', 'link']]
        update_values.append(changed_data['link'])

        # Debug prints to inspect the query and values
        print("Update query:", update_query)
        # print("Update values:", update_values)

        try:
            self.db_connection.ping(reconnect=True)
            self.cursor.execute(update_query, tuple(update_values))
            self.db_connection.commit()
            print(f"Successfully updated data for link: {changed_data['link']}")
        except Exception as e:
            self.db_connection.rollback()
            print(f"Error updating data: {str(e)}")



    def get_database_links(self) -> pd.DataFrame:
        query = "SELECT uuid, link, title, address, lat, lng, city, country, price, amenities, img_src, description, occupancy, suites, storeys, developer, price_range, one_bed_starting_from, one_bed_starting_from_unit, two_bed_starting_from, two_bed_starting_from_unit, price_per_sqft, price_per_sqft_unit, avg_price_per_sqft, avg_price_per_sqft_unit, city_avg_price_per_sqft, city_avg_price_per_sqft_unit, development_levies, parking_costs, parking_costs_unit, parkin_maintenance, parking_maintenance_unit, assignment_fee_free, storage_cost, storage_cost_unit, deposit_structure, created_at, floor_plans, Incentives FROM Precondo WHERE deleted_at IS NULL"
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
            loaction = extraction.get('location', None)
            about = extraction.get('about', None)
            amenities = extraction.get('amenities', [])
            img_urls = extraction.get('img', [])
            property_type = extraction.get('type', None)
            new_feature = extraction.get('new_feature',None)
            
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
                'price': price, 'img': img_urls, 'link': link,'title':title,
                'location':loaction,
                'updated_at': datetime.date.today().strftime('%Y/%m/%d'),
                 'about': about,'amenities': amenities,  
                'new_feature': new_feature ,
                **listing_details,
                **property_details,
                'Property_type':property_type,
            }

        except Exception as e:
            print(f'data extraction failed: {e}')

    def driver_initialization(self):
        """Initializes and returns a Selenium WebDriver."""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
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
