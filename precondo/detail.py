import requests
from bs4 import BeautifulSoup
import pandas as pd
import uuid
import datetime

def fetch_page_content(link):
    """Fetches the page content using HTTP GET."""
    try:
        response = requests.get(link)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page: {e}")
        return None

def scrape_address(soup):
    """Scrapes the address of the property."""
    try:
        address_section = soup.find('div', class_="property-title")
        address = address_section.find('p', class_="address-img fs-6").text.strip() if address_section else None
        return address
    except Exception as e:
        print(f"Error extracting address: {e}")
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
                return price
        return None
    except Exception as e:
        print(f"Error extracting price: {e}")
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
                        overview[key] = value
                    else:
                        print(f"Skipping overview item: {data_element.text.strip()} (invalid format)")
            return overview
        return {}
    except Exception as e:
        print(f"Error while getting overview details: {e}")
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
        print(f"Error while getting images: {e}")
        return []

def scrape_data(link, title, lat, lon, country):
    """Main function to scrape data from a single property listing."""
    page_content = fetch_page_content(link)
    if not page_content:
        return None

    soup = BeautifulSoup(page_content, 'html.parser')

    address = scrape_address(soup)
    price = scrape_price(soup)
    overview = scrape_overview(soup)
    img = scrape_image(soup)

    return {
        "link": link,
        "title": title,
        "address": address,
        "lat": lat,
        "lan": lon,
        "country": country,
        "price": price,
        "img_src": img,
        **overview
    }

def main():
    csv_data = pd.read_csv('api_url.csv')  # Read URL links from a CSV file

    scraped_data = []

    for index, row in csv_data.iterrows():
        if index >= 10:
            break
        link = row['link']
        title = row['title']
        lat = row['lat']
        lon = row['lon']
        country = row['country']
        try:
            data = scrape_data(link, title, lat, lon, country)
            if data:
                scraped_data.append(data)
        except Exception as e:
            print(f"Error scraping {link}: {e}")
            continue

    # Create a DataFrame and save to CSV
    if scraped_data:
        scraped_df = pd.DataFrame(scraped_data)
        scraped_df.to_csv('details.csv', index=False)
        print("Scraping completed!")
    else:
        print("No data was scraped.")

if __name__ == "__main__":
    main()
