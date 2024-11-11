from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time

# Initialize the Chrome driver in headless mode (optional for performance)
options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(options=options)

def scrape_data(driver, link):
    driver.get(link)
    
    # Wait for the page to load
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'ctl00_PageBody')))
    except Exception as e:
        print(f"Timeout waiting for page to load: {e}")
        return None

    time.sleep(2)  # Allow extra time for dynamic content

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    title = None
    address = None
    price = None
    price_unit = None
    about = None
    property_type = None
    listing_details = {}
    amenities = []

    # Scrape the title
    try:
        head = soup.find('div', id="listingtitle")
        heading = head.find('h1', class_="m-listing-title--heading")
        title = heading.find('span').text if heading and heading.find('span') else None
    except Exception as e:
        print(f"Error extracting title: {e}")

    # Scrape the address
    try:
        address_block = head.find('div', class_="address-wrap") if head else None
        address = address_block.find('span', class_="c-address").text if address_block else None
    except Exception as e:
        print(f"Error extracting address: {e}")

    # Scrape the price and price unit
    try:
        price_block = head.find_all('div', class_='c-price') if head else []
        for p in price_block:
            price_span = p.find_all('span')
            if price_span:
                price = price_span[0].text.replace('$', '').replace(',', '').strip() if price_span[0] else None
                price_unit = price_span[1].text.strip() if len(price_span) > 1 else None
                break
    except Exception as e:
        print(f"Error extracting price: {e}")
    
    #extracting property type
    try :
        property_type = soup.find('div', class_='flag-statusnew').text.strip()
    except Exception as e :
        print(f"Error extracting property type: {e}")

    # Scrape the "About" section
    try:
        description = soup.find('div', id="listingpropertydescription")
        desc = description.find('div',class_="p")
        about = desc.get_text(strip=True) if description else None
    except Exception as e:
        print(f"Error extracting about section: {e}")

    
    try:
        list_info = soup.find_all('dl', class_="listing-info__box-content")
        for item in list_info:
            key_element = item.find('dt', class_="listing-info__title")
            value_element = item.find('dd', class_="listing-info__value")
            key = key_element.text.strip() if key_element else None
            value = value_element.text.strip() if value_element else None
            if key and value:
                listing_details[key] = value
    except Exception as e:
        print(f"Error extracting listing info: {e}")
    
    # extracting amenities
    try:
        amenities_containers = soup.find_all('div', class_='prop-description__amenities')
        for container in amenities_containers:
            list_amenities = container.find('ul', class_="grid grid--spaced-all")
            if list_amenities:
                for li in list_amenities.find_all('li'):
                    amenities.append(li.text.strip())
    except Exception as e:
        print(f"Error extracting amenities: {e}")

    #extracting property deetails 
    try:
        pass
    except Exception as e:
        print(f'error extracting property details: {e}')

    return {
        'link': link,
        'title': title,
        'location': address,
        'type' : property_type,
        'price': price,
        'price_unit': price_unit,
        'about': about,
        'amenities': amenities,
        **listing_details
        
    }

def main():
    # Read the CSV containing links
    csv_data = pd.read_csv('link.csv')

    # List to store scraped data
    scraped_data = []

    # Loop through each URL in the CSV (adjust the range for testing)
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

    # Create a DataFrame from the scraped data
    if scraped_data:
        scraped_df = pd.DataFrame(scraped_data)

        # Save the DataFrame to a CSV file
        scraped_df.to_csv('details.csv', index=False)
        print("Scraping completed!")
    else:
        print("No data was scraped.")

    driver.quit()

if __name__ == "__main__":
    main()
