#iHeartJane.py
import requests
import json
import time
import random
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import toml


def fetch_data(store_id, page=0):
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.60',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Origin': 'https://www.iheartjane.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.iheartjane.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'content-type': 'application/x-www-form-urlencoded',
    'sec-ch-ua': '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': 'Windows',
    'x-algolia-api-key': 'edc5435c65d771cecbd98bbd488aa8d3',
    'x-algolia-application-id': 'VFM4X0N23A'
}

    data = json.dumps({
        "query": "",
        "filters": f"store_id = {store_id}",
        "hitsPerPage": 48,
        "userToken": "-1Focxof5Zlc304oQTuUz",
        "page": page
    }).encode()

    response = requests.post(
        'https://vfm4x0n23a-dsn.algolia.net/1/indexes/menu-products-production/query?x-algolia-agent=Algolia%20for%20JavaScript%20(4.14.2)%3B%20Browser',
        headers=headers,
        data=data,
    )

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for store {store_id}. Status code: {response.status_code} - {response.text}")
        return None

# Function to transform the JSON data into a DataFrame
def transform_data(hits, store_name, snapshot_time):
    # Extract relevant data into a list of dictionaries
    transformed = [{
        "snapshot_time": snapshot_time,
        "dispensary_name": store_name,
        "name": hit['name'],
        "price": hit.get('bucket_price'),  # Assuming 'bucket_price' exists in your data
        "quantity": hit.get('max_cart_quantity'),  # Assuming 'max_cart_quantity' exists in your data
        "type": hit.get('kind'),  # Assuming 'kind' exists in your data
        "subtype": hit.get('kind_subtype'),  # Assuming 'kind_subtype' exists in your data
        "thc_content": hit.get('percent_thc'),  # Assuming 'percent_thc' exists in your data
        "brand": hit['brand']  # The CASE logic will be handled in SQL
    } for hit in hits]
    return pd.DataFrame(transformed)

def main():
    # Connection string for SQLAlchemy
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
    store_ids = {
    # iHeartJane Stores
    888: "Cookies on the Strip",
    886: "Rise-Tropicana",
    1885: "Rise-Durango",
    1718: "Rise-Rainbow",
    887: "Rise-Henderson",
    1474: "Rise-Spanish Springs",
    1475: "Rise-Carson City",
    3417: "Rise-Reno",
    5267: "Rise-Nellis",
    5429: "Rise-Craig",
    2747: "Zen Leaf-North Las Vegas",
    1641: "Zen Leaf-Post",
    3702: "Zen Leaf-Flamingo",
    762: "Zen Leaf-Reno",
    777: "Zen Leaf-Carson City",
    3013: "The Source-Henderson",
    3012: "The Source-Rainbow",
    3104: "The Source-Northwest",
    4606: "The Source-Pahrump",
    1649: "Oasis Cannabis",
    4282: "Vegas Treehouse",
    1989: "Reef Sparks",
    2014: "Reef Sun Valley",
    1856: "Reef Reef North Las Vegas",
    1988: "Reef Reef (Western Ave.)"
    }

    snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for store_id, store_name in store_ids.items():
        page = 0
        while True:
            print(f"Fetching data for store {store_name} (ID: {store_id}), page {page}...")
            data = fetch_data(store_id, page)
            
            if data:
                hits = data.get('hits', [])
                num_records = len(hits)
                print(f"Store {store_name} (ID: {store_id}), page {page} has {num_records} records.")

                if num_records == 0:
                    print(f"No more hits for store {store_name}. Moving to next store.")
                    break

                for hit in hits:
                    # Only select the necessary fields for the DataFrame
                    selected_data = {
                        'snapshot_time': snapshot_time,
                        'Location': store_name,
                        'Product_Name': hit.get('name'),
                        'bucket_price': hit.get('bucket_price'),
                        'max_cart_quantity': hit.get('max_cart_quantity'),
                        'kind': hit.get('kind'),
                        'kind_subtype': hit.get('kind_subtype'),
                        'THC_Percent': hit.get('percent_thc'),
                        'Brand': hit.get('brand')
                    }
                    all_data.append(selected_data)

                time.sleep(random.uniform(0.5, 2))
            else:
                print(f"Failed to fetch data for store {store_name}. Exiting.")
                break

            page += 1

        time.sleep(random.uniform(5, 20))

    # Create DataFrame
    df = pd.DataFrame(all_data)

    # Define a retry mechanism for SQL bulk insert
    max_retries = 5
    retries = 0
    success = False
    while not success and retries < max_retries:
        try:
            # Bulk insert to the SQL table
            df.to_sql('iheartjane_table', engine, if_exists='append', index=False)
            success = True
            print("Bulk data inserted to SQL successfully.")
        except Exception as e:
            print(f"An error occurred during bulk insert: {e}")
            retries += 1
            time.sleep(5)  # Wait 5 seconds before retrying

    if not success:
        print("Failed to insert data to SQL after retries.")

if __name__ == "__main__":
    main()
