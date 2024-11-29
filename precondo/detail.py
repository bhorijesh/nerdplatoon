import requests
from bs4 import BeautifulSoup
import pandas as pd
import uuid
import datetime
from base.log_config import logger  
import re


COOKIES = {
    "authenticated" : "1",
    "_calendly_session": "OhOEXtFW6dtT6YPnxYgvKYUtvAE0vdYNkWHyYkrm5UDK6gdhGcD9Jv9KYvqJjZ62GTnJJ1JWjL17f4T5KcWnEhuKNDWZpz6Y02xUwyc7UmN2BijZAHrlxFyKhlJcQd0aGQt6X0R54Td92E8nYccOoVsCCw0NAB2wgnjyiVgbWOgXaxzBarfb1CqPQB7PZ2u5JXNzoHP6Iq50D70EVGH%2FrPa3fSA5l8enzuxHhScBdWy8ewWmhZdKVENa%2BwJH8c3pi7DZ7hT9irg1Je65MvmailTV4cQ0hQpgMyafPwu6bygO2bqGR5Z6GQ6c0i7mby22bR9MlnQyrIA5Wmn0YSVKCK3twQ%3D%3D--K%2FA5LhV9%2BjevkK5w--jsTzjN%2BT282HwZRl6zbNLA%3D%3D",  
    "another_cookie_name": "sBCYpGZ3az3Qr8qSXvFcJLkF9UduTwVA9SQGpTC7nXE94adrMfcgglA55VxyeZg%2BeNf2amMEPd2poSI%2BTvTYFqf3ucVVNfg%2F2DKsWvBFd7nQ0vt9kHUjgoOM36%2FSdXc2LzH1IwUR9ByXuBzX1MQHrLP6kbDjBi6IGRKW%2BicVn67C0OJapMp9Mx%2ByQ0BTQs0Qgc7q5Bd%2BhgJwk3oiXmZ0NRr%2FJtavZXH0IWXWFzR%2BmL5XG8gXkplYtDgbkeRprudcZpWROa4mtpQgUe6a1JQ0iZacifGvF%2Fkvog4LwbyGInGgeaamSmzItiBTBpdzrJg14SZ4mni8OJggbnXUqxHA9TPCPx9XZhuGHQDBX9Rg2prGly%2Bl5Oor3UfyrjVrI04tnKsrDaDw7DxfhhXG8pRLp6mPOqouKvu1MxlhIjX0Uoybw2F9ZpkDXzyWZywetfLbUiAhKIx21rimB8A60TrQZ0IkrmlK59XN9Vm5aUZ5tH8iO1YmMLdxo37nbgulsD6nwUfIKuiE1TcbKMar2a0%2FOI0N362qpTzhaUhxdIcb6%2BXXeA80kG3Iut%2FNttHR4DP1TZGKtzXwQzIyM5zTujsW0WqkFwpsmjQ8jNQV7gryGa2jArCsNzG78OW35DqooP3fzWQf967%2Fu%2FjVYGimJKjAA3TXXq27PiAvTRlV4hsbtvH2kVKp9EE77GtBWELvQ9zUrZyYAfLXD0TB0e2rGNL%2F5NCuBbaOUho%3D--l2NUKkB2AlvrqyJY--QWi1qDquCzDCBSPe8IpUrw%3D%3D"  
}

def fetch_page_content(link, cookies=COOKIES):
    """Fetches the page content using HTTP GET with optional cookies."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        response = requests.get(link, cookies=cookies, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching page: {e}")
        return None


def scrape_floor_plan(soup):
    """Dynamically scrapes floor plan details from the given HTML soup."""
    floor_plan = {}

    try:
        # Locate the container for the floor plans (assuming this is the correct section)
        floor_plan_container = soup.find("div", class_="auth")
        
        if floor_plan_container:
            ul_element = floor_plan_container.find("ul", class_="nav")
            div_elements = floor_plan_container.find("div", class_="tab-content")

            for section in floor_plan_container.find_all("h2"):
                section_key = section.text.strip() 
                if section_key == "Floor Plans":
                    section_key = "floor_plans"
                section_value = {}

                if ul_element and div_elements:
                    li_elements = ul_element.find_all("li")
                    for li in li_elements:
                        li_text = li.text.strip().replace('+','')

                        details = []  # Initialize for each `li`
                        
                        # Iterate through each floor plan data block inside `div_elements`
                        for data_item in div_elements.find_all("div", class_="tab-pane-item"):
                            img = data_item.find("div",class_="col-item col-item-photo")
                            image = img.find('img')['src'].strip() if img and img.find('img') else ''
                            title = data_item.find("div", class_="col-item-plan").text.strip() if data_item.find("div", class_="col-item col-item-plan") else ''
                            area_sqft = data_item.find("div", class_="col-item-sq-ft").text.strip() if data_item.find("div", class_="col-item-sq-ft") else ''
                            bedroom = data_item.find("div", class_="col-item col-item-bed").text.strip()

                            bedroom_data_clean = ' '.join(bedroom.split())
                            bedroom_parts = bedroom_data_clean.split(' ')
                            bedrooms = ''
                            bathrooms = ''
                            if len(bedroom_parts) > 1:
                                bedrooms = bedroom_parts[0]  
                                bathrooms = bedroom_parts[-2]  
                            else:
                                bedrooms = bedroom_parts[0] 

                            price = data_item.find("div", class_="col-item col-item-price is-bold").text.strip() if data_item.find("div", class_="col-item col-item-price is-bold") else ''
                        
                            # Append each floor plan to `details`
                            details.append({
                                "title": title,
                                "area_sqft": area_sqft,
                                "bedrooms": bedrooms,
                                "bathrooms": bathrooms,
                                "price": price,
                                "image": image
                            })

                        # Add the details to `section_value` for the current `li`
                        section_value[li_text] = details

                # Add the section to the floor plan dictionary
                floor_plan[section_key] = section_value

    except Exception as e:
        print(f"Error while scraping floor plans: {e}")
    
    return floor_plan

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
                        key = values[0].lower().replace(':','')
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
    """Processes and removes specific hyperlinks while retaining HTML tags."""
    try:
        container = soup.find('div', class_='description__box')
        
        if container:  
            for a_tag in container.find_all('a'):
                link_text = a_tag.get_text(strip=True)  
                link_url = a_tag.get('href') 

                # Remove unwanted hyperlinks
                if link_url.startswith("https://precondo.ca/") or link_url == "javascript:void(0)":
                    a_tag.decompose()  
                elif link_url: 
                    a_tag.replace_with(f"{link_text} ({link_url})") 
                
            updated_html = ''.join(str(child) for child in container.children)
            return updated_html
        return None
    except Exception as e:
        logger.error(f"Error processing hyperlinks: {e}")
        return None

    
def extract_number(value):
    clean_value = re.sub(r'[\,]', '', value)  
    match = re.search(r'\d+(\.\d+)?', clean_value)  # Search for digits (including decimals)
    return match.group() if match else None

def extract_currency_symbol(value):
    """Extracts the currency symbol from the value, if any."""
    currencies = {"$": "$", "£": "£", "€": "€"}
    for symbol in currencies:
        if symbol in value:
            return symbol
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
                currency_symbol = extract_currency_symbol(value)

                if key == 'Price Range':
                    pricing_incentive["price_range"] = None if value == "N/A" else value
                elif key == '1 Bed Starting From':
                    pricing_incentive["one_bed_starting_from"] = None if value == "Register Now" else extract_number(value)
                    pricing_incentive["one_bed_starting_from_unit"] = None if value is None or value == "Register Now" else currency_symbol
                elif key == '2 Bed Starting From':
                    pricing_incentive["two_bed_starting_from"] = None if value == "Register Now" else extract_number(value)
                    pricing_incentive["two_bed_starting_from_unit"] = None if value is None or value == "Register Now" else currency_symbol
                elif key == 'Price Per Sqft':
                    pricing_incentive["price_per_sqft"] = None if value == "N/A" else extract_number(value)
                    pricing_incentive["price_per_sqft_unit"] = None if value == "N/A" else currency_symbol
                elif key == 'City Avg Price Per Sqft':
                    pricing_incentive["city_avg_price_per_sqft"] = None if value == "N/A" else extract_number(value)
                    pricing_incentive["city_avg_price_per_sqft_unit"] = None if value is None or value == "N/A" else currency_symbol
                elif key == 'Development Levies':
                    pricing_incentive["development_levies"] = None if value == "N/A" else value
                elif key == 'Parking Cost':
                    pricing_incentive["parking_costs"] = None if value == "N/A" else extract_number(value)
                    pricing_incentive["parking_costs_unit"] = None if value == "N/A" else currency_symbol
                elif key == 'Parking Maintenance':
                    pricing_incentive["parkin_maintenance"] = None if value == "N/A" else extract_number(value)
                    pricing_incentive["parking_maintenance_unit"] = None if value == "N/A" else currency_symbol
                elif key == 'Assignment Fee Free':
                    pricing_incentive["assignment_fee_free"] = None if value == "N/A" else value
                elif key == 'Storage Cost':
                    pricing_incentive["storage_cost"] = None if value == "N/A" else extract_number(value)
                    pricing_incentive["storage_cost_unit"] = None if value == "N/A" else currency_symbol
                elif key == 'Deposit Structure':
                    pricing_incentive["deposit_structure"] = None if value == "N/A" else value
                elif key == 'Avg Price Per Sqft':
                    pricing_incentive["avg_price_per_sqft"] = None if value is None else extract_number(value)
                    pricing_incentive["avg_price_per_sqft_unit"] = currency_symbol if value is not None else None
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
    unique_id = str(uuid.uuid4())


    address = scrape_address(soup)
    price = scrape_price(soup)
    overview = scrape_overview(soup)
    img = scrape_image(soup)
    amenities = scrape_amenities(soup)
    description = scrape_description(soup)
    pricing_incentive = scrape_incentive(soup)
    floor_plan = scrape_floor_plan(soup)

    return {
        'uuid' : unique_id,
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
        **pricing_incentive,
        **floor_plan,
        'created_at': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    }

def main():
    csv_data = pd.read_csv('api_url.csv').drop_duplicates(subset=['link'])

    scraped_data = []

    for index, row in csv_data.iterrows():
        # if index >=15:
        #     break
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
        scraped_df.to_csv('details_test.csv', index=False)
        logger.info("Scraping completed!")
    else:
        logger.warning("No data was scraped.")

if __name__ == "__main__":
    main()
