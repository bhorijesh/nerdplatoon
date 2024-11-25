from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import uuid
import datetime

# Initialize the Chrome driver in headless mode (optional for performance)
options = webdriver.ChromeOptions()
options.add_argument("--headless")  
driver = webdriver.Chrome(options=options)

def wait_for_element(driver, by, value, timeout=10):
    """Wait for an element to appear on the page."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
    except Exception as e:
        print(f"Error waiting for element: {e}")

def scrape_title(soup):
    """Scrapes the title of the property."""
    try:
        head = soup.find('div', class_="property-title")
        heading = head.find('h1', class_="fw-bold fs-1 mt-0").text.strip() if head else None
        return heading
    except Exception as e:
        print(f"Error extracting title: {e}")
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
        # Find the container holding the price information
        price_container = soup.find("div", class_="property-title")
        
        # If the container is found, proceed to find the price section
        if price_container:
            price_section = price_container.find('h2')
            if price_section:
                price_span = price_section.find('span', class_='fs-1')
                if price_span:
                    price = price_span.text.strip()
                else:
                    print("Price span not found.")
                    price = None
            else:
                print("Price section (h2) not found.")
                price = None
        else:
            print("Price container (div.property-title) not found.")
            price = None
    except Exception as e:
        print(f"Error extracting price: {e}")
        price = None

    return price

def scrape_overview(soup):
    overview ={}
    try:
        overview_container = soup.find('div', class_='overview')
        overview_section = overview_container.find_all ('div' , calss_="overview-item")
        for item in overview_section:
            need_element = item.find("span")
            if need_element:
                data = need_element.split()
                key = data[0].text.strip() if data else None
                value = data[1].text.strip() if data else None
            if key and value:
                overview[key] = value
            elif key:
                overview[key] = None 
                
    except Exception as e:
        print(f"Error extracting overview:{e}")
        return overview

def scrape_data(driver, link):
    """Main function to scrape data from a single property listing."""
    driver.get(link)
    wait_for_element(driver, By.ID, 'content')  # Wait for content to load
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    title = scrape_title(soup)
    address = scrape_address(soup)
    price = scrape_price(soup)
    overview = scrape_overview(soup)

    return {
        "link" : link,
        "title": title,
        "address": address,
        "price": price,
        **overview
    }

def main():
    csv_data = pd.read_csv('url_link.csv')  # Read URL links from a CSV file

    scraped_data = []

    for index, row in csv_data.iterrows():
        if index >= 5:  # You can change this number to limit the number of links to scrape
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
