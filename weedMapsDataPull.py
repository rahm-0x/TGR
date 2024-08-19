# weedmapsDataPull.py
import requests
import json
import time
import random
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import toml  # Import the toml module

def fetch_data(store_id, page=1):
    headers = {
        'authority': 'api-g.weedmaps.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'origin': 'https://topnotch.wm.store',
        'pragma': 'no-cache',
        'referer': f'https://{store_id}.wm.store/',
        'sec-ch-ua': '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.43',
    }

    params = {
        'include[]': 'facets.tag_groups',
        'include[]': 'facets.price_weights',
        'include[]': 'facets.price_ranges',
        'page_size': '50',
        'limit': '50',
        'page': str(page),
    }
    
    url = f'https://api-g.weedmaps.com/discovery/v1/listings/dispensaries/{store_id}/menu_items'
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code} - {response.text}")
        return None

def main():
    # Load the .toml credentials
    secrets = toml.load('/Users/phoenix/Desktop/TGC-sell_through/secrets.toml')
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

    all_data = []
    snapshot_time = datetime.now().isoformat()  # Get a single timestamp for all records

    dispensaries = {
        'top-notch-thc-2': 'Top Notch THC 2',
        'curaleaf-las-vegas-boulevard': 'Curaleaf Las Vegas Boulevard',
        'curaleaf-las-vegas-western-ave': 'Curaleaf Las Vegas Western Ave'
        # Add more dispensaries here
    }

    for store_id, store_name in dispensaries.items():
        print(f"Fetching data for {store_name}...")
        page = 1

        while True:
            print(f"Fetching data for page {page}...")
            data = fetch_data(store_id, page)
            
            if data:
                hits = data.get('data', {}).get('menu_items', [])
                num_records = len(hits)
                print(f"Page {page} has {num_records} records.")

                if num_records == 0:
                    print("No more hits. Exiting.")
                    break

                for hit in hits:
                    hit['dispensary'] = store_name  # Add the new key-value pair
                    hit['snapshot_time'] = snapshot_time  # Add the snapshot time
                    all_data.append(hit)  # Append the modified dictionary to the list

                time.sleep(random.uniform(0.5, 2))
            else:
                print("Failed to fetch data. Exiting.")
                break

            page += 1

        time.sleep(random.uniform(30, 90))
    
    # Transform the data into a DataFrame
    df = pd.DataFrame(all_data)
    
    # Column mapping and transformation
    column_mapping = {
        'name': 'Product_Name',
        'brand_endorsement': 'Brand',
        'edge_category': 'Category',
        'genetics_tag': 'Strain_Type',
        'description': 'Description',
        'price': 'Price',
        'metrics': 'Metrics_Aggregates',
        'dispensary': 'Location',
        'snapshot_time': 'Snapshot_Time'
    }
    
    df = df.rename(columns=column_mapping).astype(str)
    
    # Save the data to SQL
    df.to_sql('weedmaps_table', engine, if_exists='append', index=False)
    
    print("Data saved to weedmaps_table in SQL.")

if __name__ == "__main__":
    main()
