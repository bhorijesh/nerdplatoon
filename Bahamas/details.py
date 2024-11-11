from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import uuid

# Initialize the Chrome driver in headless mode (optional for performance)
options = webdriver.ChromeOptions()
# options.add_argument("--headless")
driver = webdriver.Chrome(options=options)

def wait_for_element(driver, by, value, timeout=10):
    """Helper function to wait for an element to appear on the page."""
    try:
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
    except Exception as e:
        print(f"Error waiting for element: {e}")
        return None
    return driver



def scrape_title(soup):
    """Scrapes the title of the property."""
    try:
        head = soup.find('div', id="listingtitle")
        heading = head.find('h1', class_="m-listing-title--heading")
        title = heading.find('span').text if heading and heading.find('span') else None
    except Exception as e:
        print(f"Error extracting title: {e}")
        title = None
    return title

def scrape_address(soup):
    """Scrapes the address of the property."""
    try:
        address_block = soup.find('div', class_="address-wrap")
        address = address_block.find('span', class_="c-address").text if address_block else None
    except Exception as e:
        print(f"Error extracting address: {e}")
        address = None
    return address

def scrape_price(soup):
    """Scrapes the price and price unit."""
    price = None
    price_unit = None
    try:
        price_block = soup.find_all('div', class_='c-price')
        for p in price_block:
            price_span = p.find_all('span')
            if price_span:
                price = price_span[0].text.replace('$', '').replace(',', '').strip() if price_span[0] else None
                price_unit = price_span[1].text.strip() if len(price_span) > 1 else None
                break
    except Exception as e:
        print(f"Error extracting price: {e}")
    return price, price_unit

def scrape_about(soup):
    """Scrapes the 'About' section of the property."""
    try:
        description = soup.find('div', id="listingpropertydescription")
        desc = description.find('div', class_="p")
        about = desc.get_text(strip=True) if description else None
    except Exception as e:
        print(f"Error extracting about section: {e}")
        about = None
    return about

def scrape_listing_details(soup):
    """Scrapes additional listing details like bedrooms, bathrooms, etc."""
    listing_details = {}
    try:
        list_info = soup.find_all('dl', class_="listing-info__box-content")
        for item in list_info:
            key_element = item.find('dt', class_="listing-info__title")
            value_element = item.find('dd', class_="listing-info__value")
            key = key_element.text.strip() if key_element else None
            value = value_element.text.strip() if value_element else None
            if key in ["Full Bath", "Full Baths"]:
                listing_details["full_baths"] = value
            elif key in ["Partial Bath","Partial Baths"]:
                listing_details["partial_bath"]= value
            elif key in ["Bedrooms","Bedroom"]:
                listing_details["bedroom"] = value
            elif key and value:
                listing_details[key] = value
    except Exception as e:
        print(f"Error extracting listing info: {e}")
    return listing_details

def scrape_amenities(soup):
    amenities = None  
    try:
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
    
    return amenities


def scrape_property_details(soup, amenities):
    """Scrapes the property details section like size, year built, and appends to the amenities list."""
    property_details = {}
    if amenities is None:
        amenities =[]
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
                        if detail_key == 'Amenities' and detail_value != 'None':
                            amenities.append(detail_value)  
                        elif detail_key and detail_value:
                            section_values[detail_key] = detail_value
                if section_values:
                    property_details[section_key] = section_values
    except Exception as e:
        print(f"Error extracting property details: {e}")
    
    return property_details

def scrap_features_details(soup):
    """Scrapes the features details section """
    features_details = {}
    try:
        feature_container = soup.find('div', class_='prop-description__features  u-margin-bottom')
        if feature_container:
            for section in feature_container.find_all('h3', class_='o-title  o-title--alt  o-title-page--spaced'):
                section_key = section.text.strip()
                section_values = {}
                next_sibling = section.find_next_sibling('dl')
                if next_sibling:
                    dt_elements = next_sibling.find_all('dt', class_="prop-description__label")
                    dd_elements = next_sibling.find_all('dd', class_="prop-description__value")
                    for dt, dd in zip(dt_elements, dd_elements):
                        detail_key = dt.text.strip()
                        detail_value = dd.text.strip()
                        if detail_key and detail_value:
                            section_values[detail_key] = detail_value
                if section_values:
                    features_details[section_key] = section_values
    except Exception as e:
        print(f"Error extracting property details: {e}")
    
    return features_details


def img_url_scrap(soup):
    """Scrapes the image URLs from the property page and removes duplicates."""
    img_urls = []  
    try:
        img_container = soup.find('div', id='detail_photos_carousel_placeholder')
        
        if img_container:
            img_elements = img_container.find_all('img', attrs={'data-image-url-format': True})
            
            for img in img_elements:
                img_url = img['data-image-url-format']
                if img_url:
                    img_urls.append(img_url.strip())  

        img_urls = list(set(img_urls))

        if not img_urls:
            img_urls = None

    except Exception as e:
        print(f"Error extracting image URLs: {e}")
        img_urls = None
    
    return img_urls

def scrape_lat_lng(driver):
    """Clicks the 'Map' tab and scrapes the latitude and longitude."""
    try:
        # Click on the "Map" tab to load latitude and longitude
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, 'map_tab')))
        map_tab = driver.find_element(By.ID, "map_tab")
        map_tab.click()
        
        # Wait for the latitude and longitude fields to appear
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, 'daddr')))
        
        lat_lng_value = driver.find_element(By.NAME, 'daddr').get_attribute('value')
        
        if lat_lng_value:
            lat, lng = lat_lng_value.split(',')
            lat = lat.strip() 
            lng = lng.strip()  
            return lat, lng
        
        return None, None 
        
    except Exception as e:
        print(f"Error extracting latitude and longitude: {e}")
        return None, None

def scrape_data(driver, link):
    """Main function to scrape data from a single property listing."""
    driver.get(link)

    driver = wait_for_element(driver, By.ID, 'ctl00_PageBody')
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    link_type = link.split('/')[4]
    unique_id = str(uuid.uuid4())


    # Scrape various sections of data
    title = scrape_title(soup)
    address = scrape_address(soup)
    price, price_unit = scrape_price(soup)
    about = scrape_about(soup)
    listing_details = scrape_listing_details(soup)
    amenities = scrape_amenities(soup)
    property_details = scrape_property_details(soup, amenities)
    features_details = scrap_features_details(soup)
    lat, lng = scrape_lat_lng(driver)
    img_urls = img_url_scrap(soup) 

    return {
        'link': link,
        'title': title,
        'location': address,
        'price': price,
        'price_unit': price_unit,
        'about': about,
        'amenities': amenities,  
        'lat': lat,
        'lng': lng,
        **features_details,
        **listing_details,
        **property_details,
        'img': img_urls,
        'type': link_type,
        'uuid' : unique_id
    }

def main():
    csv_data = pd.read_csv('link.csv')

    scraped_data = []

    for index, row in csv_data.iterrows():
        if index >= 10:  
            break
        link = row['LINKS']
        try:
            data = scrape_data(driver, link)
            if data:
                scraped_data.append(data)
        except Exception as e:
            print(f"Error scraping {link}: {e}")
            continue

    # Create a DataFrame and save to CSV
    if scraped_data:
        scraped_df = pd.DataFrame(scraped_data)
        scraped_df.to_csv('details1.csv', index=False)
        print("Scraping completed!")
    else:
        print("No data was scraped.")

    driver.quit()

if __name__ == "__main__":
    main()
