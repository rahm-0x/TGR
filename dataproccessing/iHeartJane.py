import requests
import json
import time
import random
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime


def fetch_data(store_id, page=0):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Origin': 'https://www.iheartjane.com',
        'Pragma': 'no-cache',
        'Referer': 'https://www.iheartjane.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Content-Type': 'application/x-www-form-urlencoded',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'x-algolia-api-key': 'edc5435c65d771cecbd98bbd488aa8d3',
        'x-algolia-application-id': 'VFM4X0N23A'
    }

    data = json.dumps({
        "query": "",
        "filters": f"store_id = {store_id}",
        "hitsPerPage": 48,
        "userToken": "-_nZHQc8FM_xD3_vI1Pusj",
        "page": page
    }).encode()

    retries = 3
    for attempt in range(retries):
        response = requests.post(
            'https://vfm4x0n23a-dsn.algolia.net/1/indexes/menu-products-production/query?x-algolia-agent=Algolia%20for%20JavaScript%20(4.14.2)%3B%20Browser',
            headers=headers,
            data=data,
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 403:
            print(f"Forbidden (403): Check API Key and Application ID for store {store_id}")
            print(f"Response: {response.text}")
            return None
        else:
            print(f"Attempt {attempt + 1}/{retries}: Failed to fetch data for store {store_id}. Status code: {response.status_code} - {response.text}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                return None

# Function to transform the JSON data into a DataFrame
def transform_data(hits, store_name, snapshot_time):
    transformed = [{
        "snapshot_time": snapshot_time,
        "dispensary_name": store_name,
        "name": hit['name'],
        "price": hit.get('bucket_price'),  # Assuming 'bucket_price' exists in your data
        "quantity": hit.get('max_cart_quantity'),  # Assuming 'max_cart_quantity' exists in your data
        "type": hit.get('kind'),  # Assuming 'kind' exists in your data
        "subtype": hit.get('kind_subtype'),  # Assuming 'kind_subtype' exists in your data
        "thc_content": hit.get('percent_thc'),  # Assuming 'percent_thc' exists in your data
        "brand": hit['brand']
    } for hit in hits]
    return pd.DataFrame(transformed)

def main():
    engine = create_engine('postgresql+psycopg2://postgres:[pw]@[server]/tgr-main')

    all_data = []
    store_ids = {
        888: "Cookies on the Strip",
        886: "Rise-Tropicana",
        1885: "Rise-Durango",
        1718: "Rise-Rainbow",
        887: "Rise-Henderson",
        # Additional stores as needed...
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

                all_data.extend(transform_data(hits, store_name, snapshot_time).to_dict('records'))
                time.sleep(random.uniform(0.5, 2))  # Sleep to avoid rate limits
            else:
                print(f"Failed to fetch data for store {store_name}. Exiting.")
                break

            page += 1
        time.sleep(random.uniform(5, 20))

    df = pd.DataFrame(all_data)

    # Bulk insert to SQL with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            df.to_sql('iheartjane_table', engine, if_exists='append', index=False)
            print("Data inserted successfully.")
            break
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Error inserting data: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                print("Failed to insert data after multiple attempts.")

if __name__ == "__main__":
    main()
