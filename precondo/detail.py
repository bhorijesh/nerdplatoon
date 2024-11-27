import requests
from bs4 import BeautifulSoup
import pandas as pd
import uuid
import datetime
from base.log_config import logger  
import re

def fetch_page_content(link):
    """Fetches the page content using HTTP GET."""
    try:
        response = requests.get(link)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching page: {e}")
        return None

def scrape_address(soup):
    """Scrapes the address of the property."""
    try:
        address_section = soup.find('div', class_="property-title")
        address = address_section.find('p', class_="address-img fs-6").text.strip() if address_section else None
        return address
    except Exception as e:
        logger.error(f"Error extracting address: {e}")
        return None

def scrape_price(soup):
    """Scrapes the price of the property."""
    try:
        price_container = soup.find("div", class_="property-title")
        if price_container:
            price_section = price_container.find('h2')
            if price_section:
                price_span = price_section.find('span', class_='fs-1')
                price = price_span.text.strip() if price_span else None
                if price == "N/A":
                    price = None
                return price
        return None
    except Exception as e:
        logger.error(f"Error extracting price: {e}")
        return None

def scrape_overview(soup):
    """Scrapes the overview details of the property."""
    try:
        overview_con = soup.find('div', class_='overview')
        if overview_con:
            boxes = overview_con.find_all('div', class_='overview-item')
            overview = {}

            for box in boxes:
                data_element = box.find('span')
                if data_element:
                    values = data_element.text.strip().split()
                    if len(values) >= 2:
                        key = values[0].lower()
                        value = values[1]
                        overview[key] = None if value == "N/A" else value
                    else:
                        logger.warning(f"Skipping overview item: {data_element.text.strip()} (invalid format)")
            return overview
        return {}
    except Exception as e:
        logger.error(f"Error while getting overview details: {e}")
        return {}

def scrape_image(soup):
    """Scrapes the image URLs of the property."""
    try:
        img_container = soup.find('div', class_='image-gallery')
        img_src = []
        if img_container:
            img_elements = img_container.find_all('a')
            if img_elements:
                for item in img_elements:
                    img = item.get('href')
                    img_src.append(img)
        return img_src
    except Exception as e:
        logger.error(f"Error while getting images: {e}")
        return []
    
def scrape_amenities(soup):
    """Scrapes the amenities of the property."""
    try:
        amenities_first_container = soup.find('div', class_="amenities-div")
        amenities_container = amenities_first_container.find('div', class_="amenitiy") if amenities_first_container else None
        
        if amenities_container:
            amenities = []  # Initialize an empty list to hold the amenities
            for item in amenities_container.find_all('div', class_='amenity-column'):
                amenity_text = item.get_text(strip=True)
                amenities.append(amenity_text)  
            return amenities  
        else:
            return None

    except Exception as e:
        logger.error(f"Error while getting amenities: {e}")
        return None
    
def scrape_description(soup):
    """Processes and replaces hyperlinks with 'text (URL)' format, retaining HTML tags."""
    try:
        container = soup.find('div', class_='description__box')
        
        if container:  
            for a_tag in container.find_all('a'):
                link_text = a_tag.get_text(strip=True)  
                link_url = a_tag.get('href') 
                
                if link_url: 
                    a_tag.replace_with(f"{link_text} ({link_url})")  
        
            updated_html = ''.join(str(child) for child in container.children)
            return updated_html
        return None
    except Exception as e:
        logger.error(f"Error processing hyperlinks: {e}")
        return None

def scrape_incentive(soup):
    """Scrapes the incentive of the property."""
    try:
        pricing_incentive = {}
        pricing_incentive_container = soup.find('div', class_='pricing-fees')
        pricing_incentive_table = pricing_incentive_container.find('table')
        pricing_incentive_tablebody = pricing_incentive_table.find('tbody')
        
        for row in pricing_incentive_tablebody.find_all('tr'):
            cells = row.find_all('td')  # Get all <td> elements in the row
            if len(cells) >= 2:  
                key = cells[0].get_text(strip=True)  
                value = cells[1].get_text(strip=True)  
                
                if key == 'Price Range':
                    pricing_incentive["price_range"] = None if value == "N/A" else value
                elif key == '1 Bed Starting From':
                    pricing_incentive["one_bed_starting_from"] = None if value =="N/A" else value
                    pricing_incentive["one_bed_starting_from_unit"] = None if value =="N/A" else value
                elif key == '2 Bed Starting From':
                    pricing_incentive["two_bed_starting_from"] = None if value == "N/A" else value
                elif key == 'Price Per Sqft':
                    pricing_incentive["price_per_sqft"] = None if value == "N/A" else value
                elif key == 'City Avg Price Per Sqft':
                    pricing_incentive["city_avg_price_per_sqft"] = None if value == "N/A" else value
                elif key == 'Development Levies':
                    pricing_incentive["development_levies"] = None if value == "N/A" else value
                elif key == 'Parking Cost':
                    pricing_incentive["parking_cost"] = None if value == "N/A" else value
                elif key == 'Parking Maintenance':
                    pricing_incentive["parking_maintenance"] = None if value == "N/A" else value
                elif key == 'Assignment Fee Free':
                    pricing_incentive["assignment_fee_free"] = None if value == "N/A" else value
                elif key == 'Storage Cost':
                    pricing_incentive["storage_cost"] = None if value == "N/A" else value
                elif key == 'Deposit Structure':
                    pricing_incentive["deposit_structure"] = None if value == "N/A" else value
                elif key and value:
                    pricing_incentive[key] = value

        return pricing_incentive

    except Exception as e:
        logger.error(f"Error while getting incentive: {e}")

def scrape_data(link, title, lat, lon, contry, city):
    """Main function to scrape data from a single property listing."""
    page_content = fetch_page_content(link)
    if not page_content:
        return None

    soup = BeautifulSoup(page_content, 'html.parser')

    address = scrape_address(soup)
    price = scrape_price(soup)
    overview = scrape_overview(soup)
    img = scrape_image(soup)
    amenities = scrape_amenities(soup)
    description = scrape_description(soup)
    pricing_incentive = scrape_incentive(soup)

    return {
        "link": link,
        "title": title,
        "address": address,
        "lat": lat,
        "lng": lon,
        "city": city,
        "country": contry,
        "price": price,
        "amenities": amenities,
        "img_src": img,
        "description": description,
        **overview,
        **pricing_incentive
    }

def main():
    csv_data = pd.read_csv('api_url.csv').drop_duplicates(subset=['link'])

    scraped_data = []

    for index, row in csv_data.iterrows():
        if index >=30:
            break
        link = row['link']
        title = row['title']
        lat = row['lat']
        lon = row['lon']
        city = row['city']
        country = row['contry']
        try:
            logger.info(f"Scraping data for link: {link}")
            data = scrape_data(link, title, lat, lon, country, city)
            if data:
                scraped_data.append(data)
        except Exception as e:
            logger.error(f"Error scraping {link}: {e}")
            continue

    # Create a DataFrame and save to CSV
    if scraped_data:
        scraped_df = pd.DataFrame(scraped_data)
        scraped_df.to_csv('details_test1.csv', index=False)
        logger.info("Scraping completed!")
    else:
        logger.warning("No data was scraped.")

if __name__ == "__main__":
    main()
