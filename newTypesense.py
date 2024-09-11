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

# Headers and Params for API requests
headers = {
    'authority': '53r9op2dn7ha0lwip.a1.typesense.net',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'text/plain',
    'origin': 'https://nevadamademarijuana.com',
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
    # 'carrot-nevada-prod-129-loc_1-products': 'Inyo Fine Cannabis'
    # Add other collections as needed
}

# Capture snapshot timestamp for tracking daily data
snapshot_timestamp = datetime.now().isoformat()
all_data = []

# Loop through each collection and pull data
for collection, dispensary_name in collections.items():
    print(f"Starting collection: {collection} for {dispensary_name}")
    
    page = 1
    unique_hits = set()

    while True:
        # Prepare data for API request
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

        # Send API request
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
                        hit_data = hit["document"]
                        hit_data["dispensary_location"] = dispensary_name  # Add dispensary name
                        hit_data["snapshot_time"] = snapshot_timestamp  # Add snapshot timestamp
                        all_data.append(hit_data)
                print(f"Data pulled for page {page} in collection {collection}.")
            else:
                print(f"No more 'hits' data found in collection {collection}. Moving to next collection.")
                break

            # Increment page for next request
            page += 1
            time.sleep(random.uniform(0.5, 1))  # Random sleep to avoid rate limits
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break

    time.sleep(random.uniform(5, 20))  # Sleep before moving to next collection

# Create DataFrame from pulled data
if all_data:
    df = pd.DataFrame(all_data)

    # Column mapping for the SQL table
    column_mapping_typesense = {
    'name': 'Product_Name',
    'brand': 'Brand',
    'categoryName': 'Category',
    'option1Price': 'Price',
    'unitWeight': 'Quantity',
    'qty': 'Available_Quantity',  # Ensure this is pulling the right data
    'dispensary_location': 'Location',
    'snapshot_time': 'snapshot_time'
}


    # Rename columns to match the database schema
    df = df.rename(columns=column_mapping_typesense)

    # Only keep the relevant columns
    df = df[[col for col in column_mapping_typesense.values() if col in df.columns]]

    # Insert data into the SQL table
    df.to_sql('typesense_table', engine, if_exists='append', index=False)
    print("Data saved to typesense_table in SQL.")
else:
    print("No data found to save.")
