import json
import requests
import pandas as pd
from bs4 import BeautifulSoup


def all_data_extrator():
    url = "https://precondo.ca/wp-admin/admin-ajax.php"

    querystring = {"action":"list_listing","filter[]":"843","term":"163","r":"0.18986753143991475","test":"1"}

    payload = ""
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": "https://precondo.ca/",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }

    all_datas = []
    current_page = 1
    while True:
        querystring['paged'] = current_page
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

        if response.status_code != 200:
            print(f"Status : {response.status_code} for page {current_page}")
            break
        
        try:
            json_data = response.json()

            datas = json_data.get("map",[])
        except Exception as e:
            print(f"Error while changing data in json format : {e}")
            current_page += 1
            continue

        all_datas.extend(datas)

        # for next page 
        next_page_content = json_data.get('page', None)
        if next_page_content:
            soup = BeautifulSoup(next_page_content, 'lxml')

            next_page_element = soup.find('a', class_ = 'page_numbers', string=str(current_page + 1))
            if not next_page_element:
                break

        else:
            break

        current_page += 1
    with open("data.json", 'w') as file:
        json.dump(all_datas, file, indent = 4)
        
    print(f" current-page : {current_page} has {len(all_datas)} datas")
    return all_datas
            



def url_extractor(extracted_data):
    links = []
    for item in extracted_data:
        html_content = item.get('html',None)
        
        if html_content is not None:
            soup = BeautifulSoup(html_content, 'lxml')
            link_element = soup.find('a')
        
            if link_element:
                link = link_element.get('href')
                links.append(link)
    
    return links


def main():
    extracted_data = all_data_extrator()
    links = url_extractor(extracted_data)

    df = pd.DataFrame(links, columns=['link'])
    df.to_csv('urls.csv', index=False)

    print("links extraction completed")


if __name__ == "__main__":  
    main()    




            

        


              

    