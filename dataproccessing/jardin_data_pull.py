# jardin_data_pull.py
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

# Jardin dispensary ID
dispensary_id = "38392c2530769616"
dispensary_name = "Jardin"

url_template = "https://dutchie.com/graphql?operationName=FilteredProducts&variables=%7B%22includeEnterpriseSpecials%22%3Afalse%2C%22includeCannabinoids%22%3Atrue%2C%22productsFilter%22%3A%7B%22dispensaryId%22%3A%22{dispensary_id}%22%2C%22pricingType%22%3A%22rec%22%2C%22strainTypes%22%3A%5B%5D%2C%22subcategories%22%3A%5B%5D%2C%22Status%22%3A%22Active%22%2C%22types%22%3A%5B%5D%2C%22useCache%22%3Afalse%2C%22sortDirection%22%3A1%2C%22sortBy%22%3Anull%2C%22isDefaultSort%22%3Atrue%2C%22bypassOnlineThresholds%22%3Afalse%2C%22isKioskMenu%22%3Afalse%2C%22removeProductsBelowOptionThresholds%22%3Atrue%7D%2C%22page%22%3A{page}%2C%22perPage%22%3A50%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22b1e1f3db5039456c1601e5ad022be22b086bcb35d9e604113ef9a55d0cfc9bac%22%7D%7D"
headers = {'x-apollo-operation-name': 'FilteredProducts'}

all_data = []
snapshot_timestamp = datetime.now().isoformat()

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

        # Check if the response contains the product info under "properties"
        if 'data' in data and 'properties' in data['data']:
            product = data['data']['properties']
            brand = product.get('productBrand', 'N/A')
            quantity = product.get('productQuantity', 0)
            
            # Add product data to the all_data list
            all_data.append({
                'Snapshot_Timestamp': snapshot_timestamp,
                'Dispensary_ID': dispensary_id,
                'Dispensary_Name': dispensary_name,
                'ID': product.get('productId', 'N/A'),
                'Name': product.get('productName', 'N/A'),
                'Price': float(product.get('productPrice', 0)),
                'Quantity': int(quantity),
                'Type': product.get('productCannabisType', 'N/A'),
                'THC_Content': 0,  # No THC content in this response
                'Brand': brand,
                'Brand_ID': 'N/A'  # No Brand_ID in this response
            })
        else:
            print("  No products found for Jardin.")
            break  # Exit the loop since there's no data for this dispensary
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

# Apply the column mapping and datatype conversion
if not df.empty:
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
else:
    print("No data was pulled for Jardin.")
