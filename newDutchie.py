import requests
import json
import time
import random
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

# Define the list of dispensaries and their identifiers
dispensary_ids = {
    # "643710f083de0b00c09315fe": "Green Dispensary-Rainbow",
    # "5ec43b58ac891f441d3f896d": "Green Dispensary-Hualapai",
    # "61421cda0b1c9700d21b7786": "Cookies Flamingo",
    # "qwxQ9xQRwELkJBbXd": "Jardin",
    # "5fa2db295681f200aa42c5a3": "Planet 13",
    # "5e8a451e67599f00bd28d3aa": "The Dispensary-Decatur",
    # "5fc7afe97a8f4700c7404eb4": "The Dispensary-Eastern Express",
    # "5e8a4558935d7400a058d48d": "The Dispensary-Henderson",
    # "5e8a44f430483600a18c3f7b": "The Dispensary-Reno",
    # "5e850aa6c52a3100bc0ebd79": "Mynt-Downtown Reno",
    # "5e850b5bcda6ce00ae414f62": "Mynt-North",
    "mGmMapwHxh44svpjQ": "Shango",
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
    # "61ddeb5997f0d7008cf08847": "Sanctuary North Las Vegas"
}

url_template = "https://dutchie.com/graphql?operationName=FilteredProducts&variables=%7B%22includeEnterpriseSpecials%22%3Afalse%2C%22includeCannabinoids%22%3Atrue%2C%22productsFilter%22%3A%7B%22dispensaryId%22%3A%22{dispensary_id}%22%2C%22pricingType%22%3A%22rec%22%2C%22strainTypes%22%3A%5B%5D%2C%22subcategories%22%3A%5B%5D%2C%22Status%22%3A%22Active%22%2C%22types%22%3A%5B%5D%2C%22useCache%22%3Afalse%2C%22sortDirection%22%3A1%2C%22sortBy%22%3Anull%2C%22isDefaultSort%22%3Atrue%2C%22bypassOnlineThresholds%22%3Afalse%2C%22isKioskMenu%22%3Afalse%2C%22removeProductsBelowOptionThresholds%22%3Atrue%7D%2C%22page%22%3A{page}%2C%22perPage%22%3A50%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22b1e1f3db5039456c1601e5ad022be22b086bcb35d9e604113ef9a55d0cfc9bac%22%7D%7D"
headers = {'x-apollo-operation-name': 'FilteredProducts'}

all_data = []
snapshot_timestamp = datetime.now().isoformat()

# Loop through each dispensary ID
for dispensary_id, dispensary_name in dispensary_ids.items():
    print(f"Processing dispensary ID: {dispensary_id} - {dispensary_name}")
    time.sleep(random.uniform(3, 6))
    page = 1
    while True:
        print(f"  Processing page: {page}")
        url = url_template.format(dispensary_id=dispensary_id, page=page)
        response = requests.get(url, headers=headers)
        time.sleep(random.uniform(0.3, 0.7))
        if response.status_code == 200:
            data = json.loads(response.text)
            products = data['data']['filteredProducts']['products']
            if not products:
                break
            print(f"  Records pulled: {len(products)}")
            for product in products:
                thc_content = product.get('THCContent', {}).get('range', [0]) if product.get('THCContent') else [0]
                thc_content = thc_content[0] if thc_content else 0
                
                pos_data = product.get('POSMetaData', {}).get('children', [{}])[0] if product.get('POSMetaData') else {}
                quantity = pos_data.get('quantityAvailable', 0) if pos_data else 0
                
                brand = product.get('brand') or {}
                
                all_data.append({
                    'Snapshot_Timestamp': snapshot_timestamp,
                    'Dispensary_ID': dispensary_id,
                    'Dispensary_Name': dispensary_name,
                    'ID': product.get('id', 'N/A'),
                    'Name': product.get('Name', 'N/A'),
                    'Price': float(product.get('Prices', [0])[0]),
                    'Quantity': int(quantity),
                    'Type': product.get('type', 'N/A'),
                    'THC_Content': float(thc_content),
                    'Brand': brand.get('name', 'N/A'),
                    'Brand_ID': brand.get('id', 'N/A'),
                })
            page += 1
        else:
            print(f"  Request failed with status code {response.status_code}")
            break

# Transform the data into a DataFrame
df = pd.DataFrame(all_data)

# Column mapping and transformation
column_mapping = {
    'Snapshot_Timestamp': 'snapshot_timestamp',
    'Dispensary_ID': 'dispensary_id',
    'Dispensary_Name': 'dispensary_name',
    'ID': 'id',
    'Name': 'name',
    'Price': 'price',
    'Quantity': 'quantity',
    'Type': 'type',
    'THC_Content': 'thc_content',
    'Brand': 'brand',
    'Brand_ID': 'brand_id'
}

df = df.rename(columns=column_mapping).astype({
    'snapshot_timestamp': str,
    'dispensary_id': str,
    'dispensary_name': str,
    'id': str,
    'name': str,
    'price': float,
    'quantity': int,
    'type': str,
    'thc_content': float,
    'brand': str,
    'brand_id': str
})

# Save the data to SQL
df.to_sql('dispensary_inventory_snapshot', engine, if_exists='append', index=False)

print("Data saved to dispensary_inventory_snapshot in SQL.")
