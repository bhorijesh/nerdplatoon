from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from base.log_config import logger
import pandas as pd
import time

# Initialize the Chrome driver
options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(options=options)

def scrape_links_from_page():
    """
    Scrape all links from the current page.
    """
    links_set = set()
    try:
        # Parse the current page with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Locate the container with listings
        container = soup.find('div', id="loop")  # Ensure this matches the actual ID in your HTML
        if not container:
            logger.error("Listing container not found.")
            return links_set

        # Extract links from each listing entry
        listing_entries = container.find_all('div', class_="listing-etry")  
        for entry in listing_entries:
            a_tags = entry.find('a')
            if a_tags:
                link = a_tags.get('href')
                if link:
                    links_set.add(link)

    except Exception as e:
        logger.error(f"Error scraping page: {e}")
    
    return links_set

def click_next_page():
    try:
        # Parse the current page with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Locate the navigation container
        nav = soup.find('nav', class_="navigation pagination")
        if not nav:
            logger.error("Navigation container not found.")
            return False

        # Locate the 'nav-links' div inside the navigation container
        nav_links = nav.find('div', class_="nav-links")
        if not nav_links:
            logger.error("'nav-links' div not found.")
            return False

        # Locate the "Next" button inside the 'nav-links' div
        next_button_tag = nav_links.find('a', class_="next page-numbers")
        if not next_button_tag:
            logger.info("No 'Next' button found. Reached the last page.")
            return False

        # Use Selenium to locate the "Next" button by its href attribute
        next_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.LINK_TEXT, next_button_tag.text.strip()))
        )

        # Scroll the page to ensure visibility and handle overlapping
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
        time.sleep(1)  # Allow scroll to complete

        # Click the "Next" button using JavaScript to bypass click interception
        driver.execute_script("arguments[0].click();", next_button)
        time.sleep(2)  # Wait for the next page to load
        logger.info("Navigated to the next page.")
        return True

    except Exception as e:
        logger.error(f"No more pages or error navigating to the next page: {e}")
        return False

def main():
    """
    Main function to handle scraping with pagination.
    """
    links_set = set()

    # Start from the initial URL
    base_url = "https://precondo.ca/?s="  
    driver.get(base_url)
    driver.implicitly_wait(10)

    while True:
        logger.info("Scraping current page...")

        # Scrape links from the current page
        links_set.update(scrape_links_from_page())

        # Navigate to the next page
        if not click_next_page():
            break

    # Save all extracted links to a CSV file
    if links_set:
        df = pd.DataFrame(list(links_set), columns=['LINKS'])

        # Remove duplicate links
        df.drop_duplicates(subset='LINKS', inplace=True)

        df.to_csv("url_link.csv", index=False)
        logger.info("Links saved to url.csv")
    else:
        logger.warning("No links found.")

    driver.quit()

if __name__ == "__main__":
    main()
