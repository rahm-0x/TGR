import requests
import json
import time
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
cred = credentials.Certificate("/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

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

# Function to send data to Firestore
def send_to_firestore(data, collection_name):
    try:
        batch = db.batch()
        for i, record in enumerate(data):
            doc_ref = db.collection(collection_name).document()  # Auto-generate document ID
            batch.set(doc_ref, record)
            # Commit every 500 documents (Firestore batch limit)
            if (i + 1) % 500 == 0:
                batch.commit()
                batch = db.batch()
        batch.commit()
        print(f"Uploaded {len(data)} records to Firestore collection '{collection_name}'.")
    except Exception as e:
        print(f"Error uploading to Firestore: {e}")

# Main scraping and Firestore upload process
for store_id, store_name in store_ids.items():
    print(f"Processing store ID: {store_id} - {store_name}")
    page = 0
    store_data = []  # Hold all data for the current store

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
                product_data = {
                    "snapshot_time": snapshot_time,
                    "store_id": store_id,
                    "store_name": store_name,
                    "product_name": product.get("name", "Unknown Name"),
                    "price": product.get("bucket_price", "N/A"),
                    "quantity": product.get("max_cart_quantity", "N/A"),
                    "type": product.get("kind", "N/A"),
                    "thc_content": product.get("percent_thc", "N/A"),
                    "brand": product.get("brand", "Unknown Brand"),
                    "location": store_name
                }
                store_data.append(product_data)

            page += 1
        else:
            print(f"Request failed with status code {response.status_code}")
            print(response.text)
            
            if response.status_code == 422:
                print("Encountered proxy error. Waiting before retrying...")
                time.sleep(5)
            else:
                break  # Stop if the error is unrelated to proxies

    # Send data to Firestore for the current store
    if store_data:
        send_to_firestore(store_data, "iheartjane_zen")
    else:
        print(f"No data to upload for store {store_name}.")

print("All stores processed.")
