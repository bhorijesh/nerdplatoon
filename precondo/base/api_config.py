api_url = "https://precondo.ca/wp-admin/admin-ajax.php" 

# Query parameters
# querystring = {"action": "list_listing", "filter[]": "843", "term": "163"}

# payload = ""
# headers = {
#     "accept": "application/json, text/javascript, */*; q=0.01",
#     "accept-language": "en-US,en;q=0.9",
#     "priority": "u=1, i",
#     "referer": "https://precondo.ca/",
#     "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
#     "sec-ch-ua-mobile": "?0",
#     "sec-ch-ua-platform": '"Linux"',
#     "sec-fetch-dest": "empty",
#     "sec-fetch-mode": "cors",
#     "sec-fetch-site": "same-origin",
#     "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
#     "x-requested-with": "XMLHttpRequest"
# }
country_listing = {
    "ajax": {
        "url": "https://precondo.ca/new-condos-ajax/",
        "querystring": {
            "action": "list_listing",
            "term": "954",
        }
    },
    "aurora": {
        "url": "https://precondo.ca/new-condos-aurora/",
        "querystring": {
            "action": "list_listing",
            "term": "1263",
        }
    },
    "barrie" :{
        "url": "https://precondo.ca/new-condos-barrie/",
        "querystring": {    
            "action": "list_listing",
            "term": "498",
        }
    },
    "bowmanville":{
        "url": "https://precondo.ca/new-condos-bowmanville/",
        "querystring": {
            "action": "list_listing",
            "term": "982",
        }
    },
    "brampton":{
        "url": "https://precondo.ca/new-condos-brampton/",
        "querystring": {
            "action": "list_listing",
            "term": "225",
            }
    },
    "burlington":{
        "url": "https://precondo.ca/new-condos-burlington/",
        "querystring": {
            "action": "list_listing",
            "term": "171",
            }
    },
    "caledon":{
        "url": "https://precondo.ca/new-condos-caledon/",
        "querystring": {
            "action": "list_listing",
            "term": "885",
        }
    },
    "calgary-alberta":{
        "url": "https://precondo.ca/new-condos-calgary-alberta/",
        "querystring": {
            "action": "list_listing",
            "term": "1405",
            }
    },
    "cambridge":{
        "url" :"https://precondo.ca/new-condos-cambridge/",
        "querystring": {
            "action": "list_listing",
            "term": "1803",
        }
    },
    "cobourg":{
        "url": "https://precondo.ca/new-condos-cobourg/",
        "querystring": {
            "action": "list_listing",
            "term": "1225",
            }
    },
    "collingwood":{
        "url": "https://precondo.ca/new-condos-collingwood/",
        "querystring": {
            "action": "list_listing",
            "term": "705",
        }
    },
    "the-blue-mountains":{
        "url": "https://precondo.ca/new-condos-the-blue-mountains/",
        "querystring": {
            "action": "list_listing",
            "term": "992",
            }
    },
    "edmonton-alberta":{
        "url": "https://precondo.ca/new-condos-edmonton-alberta/",
        "querystring": {
            "action": "list_listing",
            "term": "1739",
        }
    },
    "etobicoke":{
        "url": "https://precondo.ca/new-condos-etobicoke/",
        "querystring": {
            "action": "list_listing",
            "term": "164",
            }
    },
    "central-etobicoke":{
        "url": "https://precondo.ca/new-condos-central-etobicoke/",
        "querystring": {
            "action": "list_listing",
            "term": "173",
            }
    },
    "mimico":{
        "url": "https://precondo.ca/new-condos-mimico/",
        "querystring": {
            "action": "list_listing",
            "term": "172",
            }
    },
    "new-condos-guelph":{
        "url": "https://precondo.ca/new-condos-guelph/",
        "querystring": {
            "action": "list_listing",
            "term": "1272",
            }
    },
    "halton-hills":{
        "url": "https://precondo.ca/new-condos-halton-hills/",
        "querystring": {
            "action": "list_listing",
            "term": "1793",
            }
    },
    "hamilton":{
        "url": "https://precondo.ca/new-condos-hamilton/",
        "querystring": {
            "action": "list_listing",
            "term": "179",
            }
    },
    "ancaster":{
        "url": "https://precondo.ca/new-condos-ancaster/",
        "querystring": {
            "action": "list_listing",
            "term": "805",
            }
    },
    "grimsby":{
        "url": "https://precondo.ca/new-condos-grimsby/",
        "querystring": {
            "action": "list_listing",
            "term": "993",
            }
    },
    "huntsville":{
        "url": "https://precondo.ca/new-condos-huntsville/",
        "querystring": {
            "action": "list_listing",
            "term": "720",
        }
    },
    "innisfil":{
        "url": "https://precondo.ca/new-condos-innisfil/",
        "querystring": {
            "action": "list_listing",
            "term": "924",
            }
    },
    "king-city":{
        "url": "https://precondo.ca/new-condos-king-city/",
        "querystring": {
            "action": "list_listing",
            "term": "1815",
            }
    },
    "kitchener":{
        "url": "https://precondo.ca/new-condos-kitchener/",
        "querystring": {
            "action": "list_listing",
            "term": "941",
            }
    },
    "laval":{
        "url": "https://precondo.ca/new-condos-laval/",
        "querystring": {
            "action": "list_listing",
            "term": "1306",
            }
    },
    "london":{
        "url": "https://precondo.ca/new-condos-london/",
        "querystring": {
            "action": "list_listing",
            "term": "1053",
            }
    },
    "markham":{
        "url": "https://precondo.ca/new-condos-markham/",
        "querystring": {
            "action": "list_listing",
            "term": "169",
            }
    },
    "milton":{
        "url": "https://precondo.ca/new-condos-milton/",
        "querystring": {
            "action": "list_listing",
            "term": "966",
            }
    },
    "mississauga":{
        "url": "https://precondo.ca/new-condos-mississauga/",
        "querystring": {
            "action": "list_listing",
            "term": "166",
            }
    },
    "erin-mills":{
        "url": "https://precondo.ca/new-condos-erin-mills/",
        "querystring": {
            "action": "list_listing",
            "term": "183",
            }
    },
    "port-credit":{
        "url": "https://precondo.ca/new-condos-port-credit/",
        "querystring": {
            "action": "list_listing",
            "term": "184",
        }
    },
    "square-one":{
        "url": "https://precondo.ca/new-condos-square-one/",
        "querystring": {
            "action": "list_listing",
            "term": "182",
            }
    },
    "montreal":{
        "url": "https://precondo.ca/new-condos-montreal/",
        "querystring": {
            "action": "list_listing",
            "term": "491",
            }
    },
    "newmarket":{
        "url": "https://precondo.ca/new-condos-newmarket/",
        "querystring": {
            "action": "list_listing",
            "term": "717",
        }
    },
    "niagara-falls":{
        "url": "https://precondo.ca/new-condos-niagara-falls/",
        "querystring": {
            "action": "list_listing",
            "term": "1257",
            }
    },
    "oakville":{
        "url": "https://precondo.ca/new-condos-oakville/",
        "querystring": {
            "action": "list_listing",
            "term": "170s",
            }
    },
    "orillia":{
        "url": "https://precondo.ca/new-condos-orillia/",
        "querystring": {
            "action": "list_listing",
            "term": "1821",
            }
    },
    "oshawa":{
        "url": "https://precondo.ca/new-condos-oshawa/",
        "querystring": {
            "action": "list_listing",
            "term": "368",
            }
    },
    "courtice":{
        "url": "https://precondo.ca/new-condos-courtice/",
        "querystring": {
            "action": "list_listing",
            "term": "686",
            }
    },
    "pickering":{
        "url": "https://precondo.ca/new-condos-pickering/",
        "querystring": {
            "action": "list_listing",
            "term": "467",  
            }
    },
    "pointe-claire":{
        "url": "https://precondo.ca/new-condos-pointe-claire/",
        "querystring": {
            "action": "list_listing",
            "term": "1243",
            }
    },
    "richmond-hill":{
        "url": "https://precondo.ca/new-condos-richmond-hill/",
        "querystring": {
            "action": "list_listing",
            "term" : "168",
        }
    },
    "scarborough":{
        "url" : "https://precondo.ca/new-condos-scarborough/",
        "querystring":{
            "action" : "list_listing",
            "term" : "167",
        }
    },
    "st-catharines":{
        "url" : "https://precondo.ca/new-condos-st-catharines/",
        "querystring":{
            "action" : "list_listing",
            "term" : "1534"
        }
    },
    "stratford":{
        "url" : "https://precondo.ca/new-condos-stratford/",
        "querystring":{
            "action" : "list_listing",
            "term": "1798",
        }
    },
    "thornhill":{
        "url" : "https://precondo.ca/new-condos-thornhill/",
        "querystring":{
            "action" :"list_listing",
            "term" : "801"
        }
    },
    "toronto":{
      "url": "https://precondo.ca/new-condos-toronto/",
      "querystring":{
          "action": "list_listing",
          "term" :"163"
      }
    },
    "don-mills-and-eglinton": {
        "url" : "https://precondo.ca/new-condos-don-mills-and-eglinton/",
        "querystring" : {
            "action" : "list_listing",
            "term": "259"
        }
    },
    "downtown-toronto":{
        "url" : "https://precondo.ca/new-condos-downtown-toronto/",
        "querystring" : {
            "action" : "list_listing",
            "term" :"174"
        }
    },
    "east" :{
        "url" : "https://precondo.ca/new-condos-downtown-toronto-east/",
        "querystring":{
            "actiom":"list_listing",
            "term" : "232",
        }
    },
    "west":{
        "url":"https://precondo.ca/new-condos-king-west/",
        "querystring": {
            "action" : "list_listing",
            "term": "177"
        }        
    },
    "riverdale":{
        "url": "https://precondo.ca/new-condos-leslieville-riverdale/",
        "querystring": {
            "action" : "list_listing",
            "term" : "217"
        }
    },
    "liberty":{
        "url" :"https://precondo.ca/new-condos-liberty-village/",
        "querystring" :{
            "action":"list_listing",
            "term":"180"
        }
    },
    "midtown":{
        "url":"https://precondo.ca/new-condos-midtown/",
        "querystring": {
            "action" : "list_listing",
            "term":"181"
        }
    },
    "northYork":{
        "url": "https://precondo.ca/new-condos-north-york/",
        "querystring":{
            "action": "list_listing",
            "term": "178",
        }
    },
    "Beaches":{
        "url" : "https://precondo.ca/new-condos-the-beaches/",
        "querystring": {
            "action":"list_listing",
            "term":"302"
        }
    },
    "waterfront":{
        "url": "https://precondo.ca/new-condos-toronto-waterfront/",
        "querystring":{
            "action": "lsist_listing",
            "term": "176"
        }
    },
    "west-toronto" : {
        "url" : "https://precondo.ca/new-condos-west-toronto/",
        "querystring":{
            "action": "list_listing",
            "term": "319"
        }
    },
    "eglinton":{
        "url" : "https://precondo.ca/new-condos-yonge-and-eglinton/",
        "querystring":{
            "action" : "list_listing",
            "term" : "236"
        }
    },
    "yorkdale":{
        "url":"https://precondo.ca/new-condos-yorkdale/",
        "querystring":{
            "action" : "list_listing",
            "term":"328"
        }
    },
    "yorkville":{
        "url" : "https://precondo.ca/new-condos-yorkville/",
        "querystring":{
            "action":"list_listing",
            "term":"175"
        }
    },
    "uxbridge":{
        "url": "https://precondo.ca/new-condos-uxbridge/",
        "querystring":{
            "action" : "list_listing",
            "term":"715"
        }
    },
    "vaughan":{
        "url":"https://precondo.ca/new-condos-vaughan/",
        "querystring": {
            "action" :"list_listing",
            "term" : "165"
        }
    },
    "woodbridge":{
        "url":"https://precondo.ca/new-condos-woodbridge/",
        "querystring":{
            "action":"list_listing",
            "term":"317"
        }
    },
    "wasaga-beach":{
        "url": "https://precondo.ca/new-condos-wasaga-beach/",
        "querystring":{
            "action": "list_listing",
            "term": "1262"
        }
    },
    "waterloo":{
        "url" : "https://precondo.ca/new-condos-waterloo/",
        "querystring": {
            "action": "list_listing",
            "term" : "942"
        }
    },
    "wellland":{
        "url" : "https://precondo.ca/new-condos-welland/",
        "querystring" : {
            "action":"list_listing",
            "term" : "1004"
        }
    },
    "whitby":{
        "url" : "https://precondo.ca/new-condos-whitby/",
        "querystring":{
            "action" : "list_listing",
            "term" :"950"
         }
    }
}

fixed_filter_value = "843"  


# Function to get dynamic query and headers for a given country
def get_dynamic_query_and_headers(country_name):
    # Get the country data from country_listing
    if country_name in country_listing:
        country_data = country_listing[country_name]
        # Extract the term from the country data for querystring
        term_value = country_data["querystring"]["term"]
        
        # Build the dynamic querystring with filter[] set to fixed value
        querystring = {
            "action": "list_listing",
            "term": term_value,
            "filter[]": fixed_filter_value,
        }
        
        # Build the dynamic headers with the referer URL set to the country's URL
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "priority": "u=1, i",
            "referer": country_data["url"],  # Use the country's URL as referer
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        return querystring, headers
    else:
        return None, None  # Return None if the country is not found
