import re
import pandas as pd
import datetime 
import requests  
import concurrent.futures   

from functools import partial
from datetime import date 
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine

from details import scrape_data
from base.db_connect import connect_database


QUERYSTRING = {"ajaxcall":"true","ajaxtarget":"privatenotes,listingmeta,customlistinginfo_attributiontext,listingdetailsmap,routeplanner,listingcommunityinfo,metainformation,listingmeta_bottom,listingmedia,listingpropertydescription,listingtabtitles,listingtools_save_bottom,customlistinginfo_commentsshort,listingtools,listingtools_mobile,listinginfo,listingmarkettrendsmodule,localguidelistingdetailspage,listingdrivetime,listingphotos,listingdetails","cms_current_mri":"119274"}
PAYLOAD = ""
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

DB_TABLE_NAME = 'bahamas'

def driver_initialization():
     options = webdriver.ChromeOptions()
     options.add_argument('--headless')
     options.add_argument('--disable-gpu')
     serivce = webdriver.ChromeService()
     driver = webdriver.Chrome(service=serivce, options=options)
     return driver

def connect_database_with_sqlalchemy():
     try:
          db_url = 'mysql+pymysql://root:@localhost:3306/practice'
          engine = create_engine(db_url)
          return engine
     except Exception as e:
          print(f'Error while connecting the database : {e}')
          
          
def fetch_link(i, driver, link: str) -> dict:
    url = f"{link}/xml-out"
    response = requests.request("GET", url, data=PAYLOAD, headers=HEADERS, params=QUERYSTRING)
    df = pd.DataFrame(columns = ['price', 'img', 'uuid', 'Partial Baths', 'Property Details', 'interior_details', 'exterior_details', 'new_features','about', 'amenities','Bedrooms','Full Baths', 'Interior','Exterior'])
    
    # print(response.status_code)
    if response.status_code == 200 and response.content.strip():  
        # Check if response content is not empty
        data_df = scrape_data(link, driver, i , df)
        price = data_df['price'].iloc[0] if not data_df['price'].empty else None
        img_urls = data_df['img_src'].iloc[0] if not data_df['img_src'].empty else None
        about = data_df['about'].iloc[0] if not data_df['about'].empty else None
        partial_baths = data_df['Partial Baths'].iloc[0] if not data_df['Partial Baths'].empty else None
        full_baths = data_df['Full Baths'].iloc[0] if not data_df['Full Baths'].empty else None 
        bedrooms = data_df['Bedrooms'].iloc[0] if not data_df['Bedrooms'].empty else None
        property_type = data_df['Property Type'].iloc[0] if not data_df['Property Type'].empty else None
        exterior = data_df['Exterior'].iloc[0] if not data_df['Exterior'].empty else None
        interior = data_df['Interior'].iloc[0] if not data_df['Interior'].empty else None
        property_details = data_df['Property Details'].iloc[0] if not data_df['Property Details'].empty else None
        interior_details = data_df['interior_details'].iloc[0] if not data_df['interior_details'].empty else None
        exterior_details = data_df['exterior_details'].iloc[0] if not data_df['exterior_details'].empty else None
        amnities_list = data_df['amenities'].iloc[0] if not data_df['amenities'].empty else None
        new_features = data_df['new_features'].iloc[0] if not data_df['new_features'].empty else None
        
        status_404 = 0
    # 205 says success but content is not available
    elif response.status_code == 205:
        price = img = about = partial_baths = full_baths = bedrooms = property_type = exterior = interior = property_details = interior_details = exterior_details = amnities_list = new_features = None
        
        status_404 = 1
    else:
        price = img_urls = about = partial_baths = full_baths = bedrooms = property_type = exterior = interior = property_details = interior_details = exterior_details = amnities_list = new_features = None
        
        status_404 = 0
        
    return {
        'price': price,
        'img_': img_urls,
        'link': link,
        'updated_at': date.today().strftime('%Y/%m/%d'),
        'status_404': status_404,
        'about': about,
        'bedrooms': bedrooms,
        'full_baths': full_baths,
        'partial_bath': partial_baths,
        'property_type': property_type,
        'amneties': str(amnities_list) if amnities_list is not None and amnities_list[0].lower() != 'none' else None,
        'exterior_details': exterior_details,
        'property_details': property_details,
        'interior_details': interior_details,
        'new_features': new_features,
        'interior': interior,
        'exterior': exterior
    }
     
     
def retrive_id_with_link(link : str) -> int:
     engine = connect_database_with_sqlalchemy()
     query = f"SELECT id, link from {DB_TABLE_NAME} where link = %s"
     df = pd.read_sql(query, engine.connect(), params=(link,))
     return df['id'].iloc[0]
     
     
def delete_not_found_items(filtered_df: dict ) -> None:
     # creating values for the query
     query_tuple = (datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"), datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"), filtered_df['link'])
     # connecting the database
     db_connection, cursor = connect_database(autocommit=True)
     db_connection.ping(reconnect=True)
     # query execution
     cursor.execute(f'UPDATE {DB_TABLE_NAME} SET deleted_at = %s, updated_at = %s WHERE link = %s', query_tuple )
     db_connection.commit()
     id_of_links_from_db = retrive_id_with_link(filtered_df['link'])
     print(f'Successfully deleted the data of link : {filtered_df['link'].iloc[0]} with id : {id_of_links_from_db}')
     
     
def get_database_links(engine) -> pd.DataFrame:
     # for bahamas
     query = "SELECT id, title, uuid, link, mls_id, web_id, price, price_unit, img_src, about, exterior, interior, bedrooms, full_baths, partial_baths, property_type, amenities, exterior_details, interior_details, property_details, new_features FROM bahamas where deleted_at is NULL"
     
     # for hgchristie
     # query = "SELECT id, title, uuid, link, mls_id, web_id, price, price_unit, img_src, about, exterior_acres, interior_sq_ft, bedrooms, full_baths, partial_baths, property_type, amneties, exterior_details, new_features, interior_details, property_details FROM hgchristie where deleted_at is NULL"
     
     df = pd.read_sql(query, con=engine)
     return df


def validation_with_db_data(db_data: pd.DataFrame, extracted_data: dict) -> tuple[dict | None, bool]:
     VALUE_CHANGED = False
     IMAGES_CHANGED = False
     changed_extracted_data = {}
     # filtering the dataframe and getting row if condition is true
     db_row = db_data[db_data['link'] == extracted_data['link']]
     # checking if the row we gto is empty or not
     if not db_row.empty:
          changed_row = {}
          
          # getting the keys from the extracted dictionary data
          for key in extracted_data.keys():
               # checking if the key is present in db_column or not
               if key in db_row.columns:
                    # getting the data value from database
                    db_value = db_row[key].iloc[0]
                    # getting the value from extracted data
                    extracted_value = extracted_data[key]
                    
                    if db_value == '':
                         db_value = None
                    if key == 'img_src':
                         # spliting the img_src string with , which gives list
                         db_value_copy = db_value.split(',') if db_value else []
                         # check newly scraped img link if changed by comparing with db_value
                         if len(extracted_value) != len(db_value_copy):
                              IMAGES_CHANGED = True
                              changed_row[key] = extracted_value
                              
                    elif key == 'new_features':
                         if extracted_value is not None and db_value is not None:
                              if len(extracted_value) != len(db_value):
                                   VALUE_CHANGED = True
                                   changed_row[key] = extracted_value
                         else:
                              if extracted_value != db_value:
                                   VALUE_CHANGED = True
                                   changed_row[key] = extracted_value
                    elif key in ['bedrooms','full_baths','partial_baths','exterior','interior','price']:
                         if extracted_value is not None and db_value is not None:
                              if str(float(extracted_value)) != str(float(db_value)):
                                   VALUE_CHANGED = True
                                   changed_row[key] = extracted_value
                              else:
                                   if str(extracted_value) != str(db_value):
                                        VALUE_CHANGED = True
                                        changed_row[key] = extracted_value
                                        
                    else:
                         if str(extracted_value) != str(db_value):
                              VALUE_CHANGED = True
                              changed_row[key] = extracted_value
                              
          if VALUE_CHANGED:
               changed_row['id'] = db_row['id'].iloc[0]     
               changed_row['link'] = extracted_data['link']
               changed_row['updated_at'] = datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S")   
               changed_extracted_data.update(changed_row)
          
          if IMAGES_CHANGED:
               changed_row['id'] = db_row['id'].iloc[0]
               changed_row['link'] = db_row['link'].iloc[0]
               changed_row['updated_at'] = datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S")
               changed_row['title'] = db_row['title'].iloc[0]
               changed_row['mls_id'] = db_row['web_id'].iloc[0]
               changed_row['web_id'] = db_row['web_id'].iloc[0]
               changed_extracted_data.update(changed_row)
     else:
          VALUE_CHANGED = True
          changed_extracted_data.update(extracted_data)    
     
     if VALUE_CHANGED or IMAGES_CHANGED:
          return changed_extracted_data, IMAGES_CHANGED
     else:
          return None, IMAGES_CHANGED      


def check_null_value(data : dict) -> dict:
     cleaned_data = {key :(None if pd.isna(value) else value) for key, value in data.items()}
     return cleaned_data


def update_data(changed_data : dict) -> None:
     db_connection,cursor = connect_database(autocommit=True)
     update_fields = [f"{key} = %s" for key in changed_data.keys() if key not in ['id', 'link']]
     update_query = f"UPDATE {DB_TABLE_NAME} SET {', '.join(update_fields)} WHERE link = %s" 
     update_values = [changed_data[key] for key in changed_data.keys() if key not in ['id', 'link']]
     update_values.append(changed_data['link'])
     try:
          db_connection.ping(reconnect=True)
          cursor.execute(update_query, tuple(update_values))
          db_connection.commit()
          print(f"Successfully updated database Data with new values for link : {changed_data['link']}")
     except Exception as e:
          # the roll back undo all the changes if any exceptions occurs
          db_connection.rollback()
          print(f'Error Occured while inserting into database in  {changed_data['link']}  : {e}')
               

def process_link(i, link, driver, db_data):
     try:
          print(i, link)
          result = fetch_link(i, driver, link)
          
          if result['status_404'] == 1:
               # update the database deleted_at column
               delete_not_found_items(result)
          else:
               # update the database with new data
               changed_data, IMAGES_CHANGED = validation_with_db_data(db_data, result)
               if changed_data is not None and isinstance(changed_data , dict):
                    # print("Change")
                    # print(changed_data)
                    update_data(changed_data)
               else:
                    print("No Change")
                    
     except Exception as e:
          print(f'Error while getting data from the link {link} : {e}')

def main():
     engine = connect_database_with_sqlalchemy()
     db_data = get_database_links(engine)
     
     links = db_data['link'].tolist()
     driver = driver_initialization()

     if len(links) > 0:
          with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
               
               # partial to pass the db_data and driver to function
               process_func = partial(process_link, db_data=db_data, driver=driver) 
               
               # execute the link correctly
               futures = {executor.submit(process_func, i, link):link for i, link in enumerate(links)}
               
               # wait fo all futures to complete and handle exceptions
               for future in concurrent.futures.as_completed(futures):
                    link = futures[future]
                    try:
                         future.result()
                    except Exception as e: 
                         print(f'Error occured while processing {link} : {e}')
               
     driver.quit()
     
if __name__ == '__main__':
     main()
     