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

# Function to scrape the links from the given page URL
def scrape_links_from_page(url):
    links_set = set()

    while True:
        try:
            print(f"Extracting links from {url}")
            driver.get(url)

            # Wait until the page content is loaded
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a.listing-item__image-container'))
            )

            # Parse the page source using BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Find all the listing items
            elements = soup.find_all('a', {'class': 'listing-item__image-container'})
            if not elements:
                print(f"No listings found on page {url}, stopping scraping this page...")
                break  

            # Extract the links from the listings and add them to the set
            for elem in elements:
                link = elem.get('href')
                if link:
                    links_set.add('https://www.hgchristie.com' + link)

            # Check for the "Next Page" button within the "paging_bottom" container
            next_con = soup.find('div', id='paging_bottom')
            if next_con:
                next_element = next_con.find('a', {"aria-label": "Next Page"})
                if next_element:
                    next_page_url = 'https://www.hgchristie.com' + next_element.get('href')
                    print(f"Next page found: {next_page_url}")
                    url = next_page_url  
                    time.sleep(3)
                else:
                    print(f"No 'Next Page' found on {url}, stopping.")
                    break  
            else:
                print(f"No pagination container found on {url}, stopping.")
                break  

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            break  

    return links_set

# Main scraping function
def main():
    links_set = set()

    # List of URLs to scrape
    urls = [
        "https://www.hgchristie.com/eng/sales/new-listings-sort/",
        "https://www.hgchristie.com/eng/sales/old-listings-sort/",
        "https://www.hgchristie.com/eng/rentals/new-listings-sort/",
        "https://www.hgchristie.com/eng/rentals/old-listings-sort/"
    ]

    # Loop through each URL
    for url in urls:
        print(f"Scraping links from {url}")
        links_set.update(scrape_links_from_page(url))  

    # Save all extracted links to a CSV file
    df = pd.DataFrame(list(links_set), columns=['LINKS'])
    df.to_csv("sale.csv", index=False)
    print("Links saved to sale.csv")


main()
driver.quit()
