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
from base.log_config import logger
from deepdiff import DeepDiff



DB_TABLE_NAME = "Precondo"  # Define your table name here

class PropertyUpdater:

    def __init__(self):
        self.db_connection, self.cursor = self.connect_database_with_pymysql()
        self.engine = self.connect_database_with_sqlalchemy()
        # self.driver = self.driver_initialization()

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
        COOKIES = {
            "authenticated": "1",
            # Add other cookies as needed
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        url = f"{link}"
        response = requests.get(link, cookies=COOKIES, headers=headers)
        response.raise_for_status()

        # Initialize base data structure for the extracted data
        extracted_data = {}

        if response.status_code == 200 and response.content.strip():
            print(f"Data fetched successfully from {url}")
            detailed_data = self.detail_extractor(link)
            if detailed_data:
                extracted_data.update(detailed_data)
            status_404 = 0
        elif response.status_code == 205:
            print(f"Content not available from {url} (Status Code 205).")
            status_404 = 1
        else:
            print(f"Failed to fetch data from {url} - Status code: {response.status_code}")
            status_404 = 0
            extracted_data = {'status_404': status_404, 'link': link}

        return extracted_data

    def validating_with_db_data(self, db_data: pd.DataFrame, extracted_data: dict) -> tuple[dict | None, bool]:
        VALUE_CHANGED = False
        IMAGES_CHANGED = False
        changed_extracted_data = {}
        print(f"Validating link: {extracted_data['link']}")

        db_row = db_data[db_data['link'] == extracted_data['link']]

        if not db_row.empty:
            print(f"Link {extracted_data['link']} found in database.")
            changed_row = {}
            upper = ['occupancy', 'developer', 'deposit_structure']
            sqft = [
                'one_bed_starting_from', 'storeys', 'two_bed_starting_from',
                'price_per_sqft', 'avg_price_per_sqft', 'city_avg_price_per_sqft',
                'parkin_costs', 'parkin_maintenance', 'storage_cost'
            ]

            for key in extracted_data.keys():
                if key in db_row.columns:
                    db_value = db_row[key].iloc[0]
                    extracted_value = extracted_data[key]

                    if db_value == '':
                        db_value = None
                    if key == 'img_src' or key == 'amenities':
                        # Handle None values before proceeding
                        db_value_copy = db_value.split(',') if db_value else []
                        extracted_value_copy = extracted_value if extracted_value is not None else []
                        if len(extracted_value_copy) != len(db_value_copy):
                            IMAGES_CHANGED = True
                            changed_row[key] = extracted_value
                            logger.info(f"{extracted_data['link']} :: {key} CHANGED FROM {db_value} TO {extracted_value}")
                            
                    elif key == 'floor_plans':
                         diff = DeepDiff(db_value, extracted_value, ignore_order=True).to_dict()
                         if diff:
                            VALUE_CHANGED = True
                            changed_row[key] = extracted_value
                            logger.info(f"{extracted_data['link']} :: {key} CHANGED FROM {len(db_value)} TO {len(extracted_value)}")
                         else:
                            logger.info(f"{extracted_data['link']} :: {key} Not CHANGED ")

                    elif key in upper: 
                        # Handle NaN values in upper keys section
                        if pd.isna(db_value) and pd.isna(extracted_value):
                            continue 
                        elif pd.isna(db_value) or pd.isna(extracted_value):
                            VALUE_CHANGED = True
                            changed_row[key] = extracted_value
                            logger.info(f"{extracted_data['link']} :: {key} CHANGED FROM {len(db_value_copy)} TO {len(extracted_value)}")

                        elif extracted_value != db_value:
                            VALUE_CHANGED = True
                            changed_row[key] = extracted_value
                            logger.info(f"{extracted_data['link']} :: {key} CHANGED FROM {len(db_value_copy)} TO {len(extracted_value)}")
                            
                    elif key in sqft:
                        if extracted_value is not None and db_value is not None:
                            if str(float(extracted_value)) != str(float(db_value)):
                                VALUE_CHANGED = True
                                changed_row[key] = extracted_value
                                logger.info(f"{extracted_data['link']} :: {key} CHANGED FROM {len(db_value_copy)} TO {len(extracted_value)}")
                            
                            
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
        try:
            # Ensure `img_src` is serialized as JSON string if it's a list
            if 'img_src' in changed_data and isinstance(changed_data['img_src'], list):
                changed_data['img_src'] = json.dumps(changed_data['img_src'])
            
            # Ensure `amenities` is serialized as JSON string if it's a list or dict
            if 'amenities' in changed_data and isinstance(changed_data['amenities'], (list, dict)):
                changed_data['amenities'] = json.dumps(changed_data['amenities'])
            
            # Prepare update fields, excluding 'id' and 'link'
            update_fields = [f"{key} = %s" for key in changed_data.keys() if key not in ['id', 'link']]
            if not update_fields:
                print("No fields to update.")
                return
            
            # Prepare the SQL update query
            update_query = f"UPDATE {DB_TABLE_NAME} SET {', '.join(update_fields)} WHERE link = %s"
            
            # Prepare the corresponding values
            update_values = [changed_data[key] for key in changed_data.keys() if key not in ['id', 'link']]
            update_values.append(changed_data['link'])
            
            # Execute the update query
            self.db_connection.ping(reconnect=True)
            self.cursor.execute(update_query, tuple(update_values))
            self.db_connection.commit()
            
            print(f"Successfully updated data for link: {changed_data['link']}")
        except Exception as e:
            self.db_connection.rollback()
            print(f"Error updating data: {str(e)}")




    def get_database_links(self) -> pd.DataFrame:
        query = "SELECT id,uuid, link, address, price, amenities , img_src, description, occupancy, suites, storeys, developer, price_range, one_bed_starting_from, one_bed_starting_from_unit, two_bed_starting_from, two_bed_starting_from_unit, price_per_sqft, price_per_sqft_unit, avg_price_per_sqft, avg_price_per_sqft_unit, city_avg_price_per_sqft, city_avg_price_per_sqft_unit, development_levies, parking_costs, parking_costs_unit, parkin_maintenance, parking_maintenance_unit, assignment_fee_free, storage_cost, storage_cost_unit, deposit_structure, created_at, floor_plans, Incentives FROM Precondo WHERE deleted_at IS NULL"
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
            extraction = scrape_data(link)

            # Extract necessary data from the scraped information
            price = extraction.get('price', None)
            amenities = extraction.get('amenities', [])
            img_urls = extraction.get('img_src', [])
            floor_plans = extraction.get('floor_plans',{})
            overview_keys = ['occupancy','suites','storeys','developer']
            overview = {key: extraction.get(key) for key in overview_keys}
            prefixes = ["price_", "parking_", "storage_", "avg_", "incentive", "city_", "dev","one_","two_",'In']
            pricing_incentive = {key: extraction.get(key) for key in extraction if any(key.startswith(prefix) for prefix in prefixes)}
            floor_plans_str = json.dumps(floor_plans) if floor_plans else None

            # Initialize the result dictionary
            result = {
                'price': price,
                'img_src': img_urls,
                'link': link,
                'updated_at': datetime.date.today().strftime('%Y/%m/%d'),
                'amenities': amenities,
                'floor_plans' : floor_plans_str,
                **overview,
                **pricing_incentive,
            }

            return result

        except Exception as e:
            print(f'Data extraction failed: {e}')
            return None



    # def driver_initialization(self):
    #     """Initializes and returns a Selenium WebDriver."""
    #     options = webdriver.ChromeOptions()
    #     options.add_argument('--headless')
    #     options.add_argument('--disable-gpu')
    #     driver = webdriver.Chrome(options=options)
    #     return driver

    def main(self):
        df = self.get_database_links()
        for i, link in enumerate(df['link']):
            # Sequentially call fetch_link with index and link
            data = self.fetch_link(i, link)
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
