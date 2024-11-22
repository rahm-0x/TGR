import requests
import json
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import toml

# Load database credentials from secrets.toml
secrets = toml.load('secrets.toml')
db_config = {
    "user": secrets['user'],
    "password": secrets['password'],
    "host": secrets['host'],
    "port": secrets['port'],
    "dbname": secrets['dbname']
}

# Connection string for SQLAlchemy
engine = create_engine(
    f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["dbname"]}'
)

# ZenRows API Key and endpoint
ZENROWS_API_KEY = "6d31be69d8761dff28f1aad409b3b563f9a2b9f9"
zenrows_url = "https://api.zenrows.com/v1/"

# Define the Dutchie GraphQL endpoint and query data
graphql_endpoint = "https://dutchie.com/graphql"

# Set up ZenRows request parameters for POST
headers = {
    "Content-Type": "application/json"
}
params = {
    "apikey": ZENROWS_API_KEY,
    "url": graphql_endpoint,
    "js_render": "true",
    "json_response": "true"
}

# Dispensing and pagination variables
dispensary_ids = {
    "643710f083de0b00c09315fe": "Green Dispensary-Rainbow",
    "5ec43b58ac891f441d3f896d": "Green Dispensary-Hualapai",
    "61421cda0b1c9700d21b7786": "Cookies Flamingo",
    # "qwxQ9xQRwELkJBbXd": "Jardin",
    # "5fa2db295681f200aa42c5a3": "Planet 13",
    # "5e8a451e67599f00bd28d3aa": "The Dispensary-Decatur",
    # "5fc7afe97a8f4700c7404eb4": "The Dispensary-Eastern Express",
    # "5e8a4558935d7400a058d48d": "The Dispensary-Henderson",
    # "5e8a44f430483600a18c3f7b": "The Dispensary-Reno",
    # "5e850aa6c52a3100bc0ebd79": "Mynt-Downtown Reno",
    # "5e850b5bcda6ce00ae414f62": "Mynt-North",
    # "mGmMapwHxh44svpjQ": "Shango",
    # "621d609dcf42ba00a755f9cf": "Deep Roots-Cheyenne",
    # "61525b4a98002d00b2c3cefd": "Deep Roots-Blue Diamond",
    # "621d3e559ee6fa008444a2b9": "Deep Roots-Mesquite",
    # "621d600071ee1b00831cf13a": "Deep Roots-Wendover",
    # "61525afd7b310a00d403e3b1": "Deep Roots-North Las Vegas",
    # "B5EwKP4GwjofpXCc8": "Thrive-Reno",
    # "KsKwLNXcCcdCXPr7D": "Thrive-West Sahara",
    # "tuzk3PdHpDbKeAXw5": "Thrive-Jackpot",
    # "5f18c1fa019920013749b8ce": "Thrive-Cactus",
    # "G58JLxJSWkfiwF7KD": "Thrive-Cheyenne",
    # "61d6016935aacc00a23b0d07": "Thrive-Sammy Davis",
    # "6125d6766b720a00d333ba81": "Sahara Wellness",
    # "61ddeb052ac5e60092323aec": "Sanctuary Downtown Las Vegas",
    # "61ddeb5997f0d7008cf08847": "Sanctuary North Las Vegas",
    # "6074bf74331e7d00b3e62560": "Curaleaf Las Vegas Blvd",
    # "6074bf0b882eee00bbcf09eb": "Curaleaf Western Ave"
}

# Get current snapshot time
snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Initialize a list to hold all product data
all_data = []

# Process each dispensary
for dispensary_id, dispensary_name in dispensary_ids.items():
    print(f"Processing dispensary ID: {dispensary_id} - {dispensary_name}")
    page = 0
    
    while True:
        print(f"  Processing page: {page}")
        payload = {
            "operationName": "FilteredProducts",
            "variables": {
                "includeEnterpriseSpecials": False,
                "includeCannabinoids": True,
                "productsFilter": {
                    "dispensaryId": dispensary_id,
                    "Status": "Active"
                },
                "page": page,
                "perPage": 50
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "0bad4e699e72491f25da4a03bcc6ae15a1a1a91aebb39a5ef055a5a6314daaca"
                }
            }
        }

        # Make the request
        response = requests.post(zenrows_url, params=params, headers=headers, json=payload)
        time.sleep(2)  # Delay to avoid rate limiting
        
        if response.status_code == 200:
            data = response.json()
            products = data.get("data", {}).get("filteredProducts", {}).get("products", [])
            
            if not products:
                print(f"No more products found on page {page}. Ending scrape for {dispensary_name}.")
                break

            print(f"  Records pulled: {len(products)}")
            
            for product in products:
                product_name = product.get("Name") or product.get("name") or "Unknown Name"
                price = product.get("Prices", [None])[0]
                brand = product.get("brand", {})
                brand_name = brand.get("name", "Unknown Brand") if isinstance(brand, dict) else "Unknown Brand"
                type_ = product.get("type") or product.get("Type") or "N/A"
                
                # Extract quantity
                quantity = product.get("POSMetaData", {}).get("children", [{}])[0].get("quantityAvailable", None)

                # Append product details to all_data
                all_data.append({
                    "snapshot_time": snapshot_time,
                    "dispensary_id": dispensary_id,
                    "dispensary_name": dispensary_name,
                    "product_name": product_name,
                    "price": price,
                    "brand": brand_name,
                    "type": type_,
                    "quantity": quantity
                })

            page += 1  # Move to the next page
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
        df.to_sql('dutchieZen', engine, if_exists='append', index=False)
        print("Data successfully written to the database.")
    except Exception as e:
        print(f"Error writing to the database: {e}")
else:
    print("No data to write to the database.")
