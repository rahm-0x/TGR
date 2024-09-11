import requests
import json
import time
import random
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import toml

# Load database credentials from secrets.toml
secrets = toml.load('/Users/phoenix/Desktop/TGR/secrets.toml')
db_config = {
    "user": secrets['user'],
    "password": secrets['password'],
    "host": secrets['host'],
    "port": secrets['port'],
    "dbname": secrets['dbname']
}

# Connection string for SQLAlchemy
user = db_config['user']
password = db_config['password']
host = db_config['host']
port = db_config['port']
database = db_config['dbname']

engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

# Headers and Params
headers = {
    'authority': '53r9op2dn7ha0lwip.a1.typesense.net',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'text/plain',
    'origin': 'https://nevadamademarijuana.com',
    'pragma': 'no-cache',
    'referer': 'https://nevadamademarijuana.com/',
    'sec-ch-ua': '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.31',
}

params = {
    'x-typesense-api-key': '6nObvdm8Ao0rlQvJwQBZIanLrd4ZzBFF',
}

# Dictionary of collections to loop through, mapped to dispensary names
collections = {
    'carrot-nevada-prod-3-loc_1-products': 'Nevada Made Marijuana - Charleston',
    'carrot-nevada-prod-3-loc_4-products': 'Nevada Made Marijuana-Laughlin',
    'carrot-nevada-prod-3-loc_3-products': 'Nevada Made Marijuana-Warm Springs',
    'carrot-nevada-prod-3-loc_2-products': 'Nevada Made Marijuana-Henderson',
    'carrot-nevada-prod-50-loc_2-products': 'Tree of Life-North',
    'carrot-nevada-prod-50-loc_1-products': 'Tree of Life-Jones',
    'carrot-wallflower-prod-1-loc_1-products': 'Wallflower',
    'carrot-nevada-prod-1-loc_1-products': 'Euphoria',
    'carrot-nevada-prod-49-loc_1-products': 'Exhale',
    'carrot-nevada-prod-129-loc_1-products': 'Inyo Fine Cannabis',
    'carrot-silversage-prod-1-loc_1-products': 'Silver Sage Wellness',
    'carrot-showgrow-prod-1-loc_1-products': 'showgrow',
    'carrot-nevada-prod-167-loc_1-products': 'Greenleaf Wellness Reno'
}

all_data = []

for collection, dispensary_name in collections.items():
    print(f"Starting collection: {collection}")

    # Initialize page variable and set to store unique IDs
    page = 1
    unique_hits = set()

    while True:
        print(f"Pulling data for page {page} in collection {collection}...")  # Print current page

        # Update the data payload with the current page number
        data = json.dumps({
            "searches": [{
                "query_by": "name, brand, strain, masterCategoryName, description, labResultNames, slug",
                "infix": "always, always, off, off, off, always, off",
                "per_page": 100,
                "exhaustive_search": True,
                "collection": collection,
                "q": "*",
                "facet_by": "labResultNames,masterCategoryName,brand,strain,option1Price,thcPercentage",
                "sort_by": "option1Price:asc",
                "max_facet_values": 1000,
                "page": page
            }]
        })

        response = requests.post('https://53r9op2dn7ha0lwip.a1.typesense.net/multi_search', params=params, headers=headers, data=data)

        if response.status_code == 200:
            json_data = response.json()

            try:
                hits_data = json_data["results"][0]["hits"]
            except (KeyError, IndexError):
                hits_data = []

            if hits_data:
                for hit in hits_data:
                    hit_id = hit["document"]["id"]
                    if hit_id not in unique_hits:
                        unique_hits.add(hit_id)
                        hit["document"]["dispensary_location"] = dispensary_name  # Add dispensary name
                        all_data.append(hit)
                print(f"Data pulled for page {page} in collection {collection}.")
            else:
                print(f"No more 'hits' data found in collection {collection}. Moving to next collection.")
                break

            # Increment the page number
            page += 1

            # Random sleep to avoid rate limits
            sleep_time = random.uniform(0.5, 1)
            time.sleep(sleep_time)
            
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break

    # Random sleep before moving to the next collection
    sleep_time = random.uniform(5, 20)
    print(f"Waiting {sleep_time:.2f} seconds before moving to the next collection.")
    time.sleep(sleep_time)

# Save all unique hits to a single JSON file
snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

if all_data:
    with open("typesenseAllDataPull.json", "w") as json_file:
        json.dump(all_data, json_file, indent=4)
    print("All unique data saved to typesenseAllDataPull")

# Load the Typesense JSON data
with open('typesenseAllDataPull.json', 'r') as f:
    typesense_data = json.load(f)

# Flatten the 'document' field and create a DataFrame
flattened_data = [item['document'] for item in typesense_data]
df_typesense = pd.DataFrame(flattened_data)

# Drop any unnecessary columns
if 'visibleTags' in df_typesense.columns:
    df_typesense = df_typesense.drop(columns=['visibleTags'])

# Add snapshot_time column
df_typesense['snapshot_time'] = snapshot_time

# Column mapping for typesense data
column_mapping_typesense = {
    'name': 'Product_Name',
    'brand': 'Brand',
    'categoryName': 'Category',
    'option1Price': 'Price',
    'unitWeight': 'Quantity',
    'qty': 'Available_Quantity',
    'dispensary_location': 'Location',
    'snapshot_time': 'snapshot_time'
}

# Remove any columns that are not in the column_mapping
df_typesense = df_typesense.rename(columns=column_mapping_typesense)
df_typesense = df_typesense[[col for col in column_mapping_typesense.values() if col in df_typesense.columns]]

# Transform and save to the SQL table
df_typesense.to_sql('typesense_table', engine, if_exists='append', index=False)

print("Data saved to typesense_table in SQL.")
