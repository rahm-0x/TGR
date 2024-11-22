import requests
import json
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
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

# ZenRows API Key and endpoint
ZENROWS_API_KEY = "6d31be69d8761dff28f1aad409b3b563f9a2b9f9"
zenrows_url = "https://api.zenrows.com/v1/"

# Set up ZenRows request parameters
headers = {
    "Content-Type": "application/json"
}
params = {
    "apikey": ZENROWS_API_KEY,
    "js_render": "true",
    "json_response": "true"
}

# Define iHeartJane stores and snapshot time
store_ids = {
    888: "Cookies on the Strip",
    886: "Rise-Tropicana",
    # 1885: "Rise-Durango",
    # 1718: "Rise-Rainbow",
    # 887: "Rise-Henderson",
    # 1474: "Rise-Spanish Springs",
    # 1475: "Rise-Carson City",
    # 3417: "Rise-Reno",
    # 5267: "Rise-Nellis",
    # 5429: "Rise-Craig",
    # 2747: "Zen Leaf-North Las Vegas",
    # 1641: "Zen Leaf-Post",
    # 3702: "Zen Leaf-Flamingo",
    # 762: "Zen Leaf-Reno",
    # 777: "Zen Leaf-Carson City",
    # 3013: "The Source-Henderson",
    # 3012: "The Source-Rainbow",
    # 3104: "The Source-Northwest",
    # 4606: "The Source-Pahrump",
    # 1649: "Oasis Cannabis",
    # 4282: "Vegas Treehouse",
    # 1989: "Reef Sparks",
    # 2014: "Reef Sun Valley",
    # 1856: "Reef Reef North Las Vegas",
    # 1988: "Reef Reef (Western Ave.)"
}
snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Initialize a list to hold all product data
all_data = []

for store_id, store_name in store_ids.items():
    print(f"Processing store ID: {store_id} - {store_name}")
    page = 0
    
    while True:
        print(f"  Processing page: {page}")
        data_payload = json.dumps({
            "query": "",
            "filters": f"store_id:{store_id}",
            "hitsPerPage": 48,
            "page": page,
            "userToken": "tMwhGOkA1eGNh43F38Y7f"
        })

        response = requests.post(
            f"{zenrows_url}?url=https://search.iheartjane.com/1/indexes/menu-products-production/query",
            headers=headers,
            params=params,
            data=data_payload
        )
        time.sleep(2)

        if response.status_code == 200:
            data = response.json()
            products = data.get("hits", [])
            
            if not products:
                print(f"No more products found on page {page}. Ending scrape for {store_name}.")
                break

            print(f"  Records pulled: {len(products)}")
            
            for product in products:
                product_name = product.get("name", "Unknown Name")
                price = product.get("bucket_price", None)
                quantity = product.get("max_cart_quantity", None)
                kind = product.get("kind", None)
                thc_content = product.get("percent_thc", None)
                brand = product.get("brand", "Unknown Brand")
                location = store_name  # Use store name as location
                
                # Append product details to all_data
                all_data.append({
                    "snapshot_time": snapshot_time,
                    "store_name": store_name,
                    "product_name": product_name,
                    "price": price,
                    "quantity": quantity,
                    "type": kind,
                    "thc_content": thc_content,
                    "brand": brand
                })

            page += 1
        else:
            print(f"Request failed with status code {response.status_code}")
            print(response.text)
            
            if response.status_code == 422:
                print("Encountered proxy error. Waiting before retrying...")
                time.sleep(5)
            else:
                break  # Stop if the error is unrelated to proxies

# Convert the data into a pandas DataFrame
df = pd.DataFrame(all_data)

# Write the data to the PostgreSQL database
if not df.empty:
    try:
        df.to_sql('IheartJaneZen', engine, if_exists='append', index=False)
        print("Data successfully written to the database.")
    except Exception as e:
        print(f"Error writing to the database: {e}")
else:
    print("No data to write to the database.")
