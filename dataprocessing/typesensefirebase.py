import requests
import json
import time
import random
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
cred = credentials.Certificate("/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# Headers and Params (without HTTP/2 pseudo-headers)
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'text/plain',
    'origin': 'https://store.showgrowvegas.com',
    'referer': 'https://store.showgrowvegas.com/',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
}

params = {
    'x-typesense-api-key': '6nObvdm8Ao0rlQvJwQBZIanLrd4ZzBFF',
}

# Dictionary of collections to loop through, mapped to dispensary names
collections = {
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

# Function to send data to Firestore
def send_to_firestore(data, collection_name):
    try:
        batch = db.batch()
        for i, record in enumerate(data):
            doc_ref = db.collection(collection_name).document()  # Auto-generate document ID
            batch.set(doc_ref, record)
            if (i + 1) % 500 == 0:  # Commit in batches of 500
                batch.commit()
                batch = db.batch()
        batch.commit()
        print(f"Uploaded {len(data)} records to Firestore collection '{collection_name}'.")
    except Exception as e:
        print(f"Error uploading to Firestore: {e}")

# Initialize snapshot time
snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Process each collection
for collection, dispensary_name in collections.items():
    print(f"Starting collection: {collection}")
    page = 1
    unique_hits = set()
    all_data = []

    while True:
        print(f"Pulling data for page {page} in collection {collection}...")

        # Prepare data payload
        data_payload = json.dumps({
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

        response = requests.post('https://53r9op2dn7ha0lwip.a1.typesense.net/multi_search', params=params, headers=headers, data=data_payload)

        if response.status_code == 200:
            json_data = response.json()
            hits_data = json_data.get("results", [{}])[0].get("hits", [])

            if hits_data:
                for hit in hits_data:
                    hit_id = hit["document"]["id"]
                    if hit_id not in unique_hits:
                        unique_hits.add(hit_id)
                        document = hit["document"]
                        document["Location"] = dispensary_name  # Add dispensary name
                        document["snapshot_time"] = snapshot_time  # Add snapshot time
                        all_data.append(document)
                print(f"Data pulled for page {page} in collection {collection}.")
            else:
                print(f"No more hits found in collection {collection}.")
                break

            page += 1
            time.sleep(random.uniform(0.5, 1))  # Random sleep to avoid rate limits
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            print(response.text)
            break

    # Upload data to Firestore
    if all_data:
        send_to_firestore(all_data, "typesense_table")
    else:
        print(f"No data to upload for collection {collection}.")

    time.sleep(random.uniform(5, 10))  # Random sleep before moving to the next collection

print("All collections processed.")
