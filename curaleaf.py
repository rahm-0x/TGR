import requests
import json
import time
import random
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
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
Session = sessionmaker(bind=engine)
session = Session()

# Database table setup
Base = declarative_base()

class CuraleafData(Base):
    __tablename__ = 'curaleaf_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_timestamp = Column(DateTime)
    dispensary_name = Column(String)
    name = Column(String)
    price = Column(Float)
    quantity = Column(Integer)
    type = Column(String)
    subtype = Column(String)
    thc_content = Column(String)
    brand = Column(String)

# Create the table in the database
Base.metadata.create_all(engine)

# API setup
url = 'https://graph.curaleaf.com/api/curaql'
headers = {
    'authority': 'graph.curaleaf.com',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://curaleaf.com',
    'pragma': 'no-cache',
    'referer': 'https://curaleaf.com/',
    'user-agent': 'Mozilla/5.0'
}

dispensary_ids = [
    {"id": "LMR054", "name": "Curaleaf Western Ave"},
    {"id": "LMR019", "name": "Curaleaf Las Vegas Blvd"}
]

unique_records = set()

for dispensary in dispensary_ids:
    all_records = []
    page = 1
    new_records_this_page = 0

    while True:
        time.sleep(random.uniform(2, 2.5))

        payload = {
            "operationName": "PGP",
            "variables": {
                "dispensaryUniqueId": dispensary['id'],
                "menuType": "RECREATIONAL"
            },
            "query": """fragment grid on Product {
                brand {
                    name
                }
                cardDescription
                category {
                    displayName
                    key
                    __typename
                }
                effects {
                    displayName
                    __typename
                }
                id
                images {
                    url
                    __typename
                }
                labResults {
                    thc {
                        formatted
                        range
                        __typename
                    }
                    terpenes {
                        terpene {
                            name
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
                name
                offers {
                    id
                    title
                    __typename
                }
                strain {
                    key
                    displayName
                    __typename
                }
                subcategory {
                    key
                    displayName
                    __typename
                }
                variants {
                    id
                    isSpecial
                    option
                    price
                    quantity
                    specialPrice
                    __typename
                }
                __typename
            }
            query PGP($dispensaryUniqueId: ID!, $menuType: MenuType!) {
                dispensaryMenu(dispensaryUniqueId: $dispensaryUniqueId, menuType: $menuType) {
                    offers {
                        id
                        title
                        __typename
                    }
                    products {
                        ...grid
                        __typename
                    }
                    __typename
                }
            }"""
        }

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        products = data.get('data', {}).get('dispensaryMenu', {}).get('products', [])

        print(f"Pulling data for page {page}, found {len(products)} records.")

        for product in products:
            name = product.get('name')

            if name not in unique_records:
                unique_records.add(name)
                new_records_this_page += 1

                record = {
                    'snapshot_timestamp': datetime.now(),
                    'dispensary_name': dispensary['name'],
                    'name': product.get('name'),
                    'price': product.get('variants', [{}])[0].get('price'),
                    'quantity': product.get('variants', [{}])[0].get('quantity'),
                    'type': product.get('category', {}).get('key'),
                    'subtype': product.get('subcategory', {}).get('key') if product.get('subcategory') else None,
                    'thc_content': product.get('labResults', {}).get('thc', {}).get('formatted'),
                    'brand': product.get('brand', {}).get('name') if product.get('brand') else None
                }
                all_records.append(record)

        print(f"Added {new_records_this_page} new records.")

        if new_records_this_page == 0:
            print("No new records found. Exiting loop.")
            break

        new_records_this_page = 0
        page += 1

    for record in all_records:
        new_record = CuraleafData(**record)
        session.add(new_record)

    session.commit()

    sleep_time = random.uniform(60, 120)  # 1 to 2 minutes
    print(f"Sleeping for {sleep_time} seconds before moving on to the next dispensary.")
    time.sleep(sleep_time)

print("Data successfully inserted into the table.")
