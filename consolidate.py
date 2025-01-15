import pandas as pd
from firebase_admin import credentials, firestore, initialize_app
from datetime import datetime

# Initialize Firebase Admin SDK
cred = credentials.Certificate("/Users/phoenix/Desktop/TGR-Firebase/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
initialize_app(cred)
db = firestore.client()

# Field mappings for each collection
FIELD_MAPPING = {
    "typesense_table_TGCONLY": {
        "price": "option1Price",
        "brand": "brand",
        "quantity": "qty",
        "dispensary_name": "Location",
        "category": "categoryName",
        "snapshot_time": "snapshot_time",
        "product_name": "name"
    },
    "dutchie_zen_TGCONLY": {
        "price": "price",
        "brand": "brand",
        "quantity": "quantity",
        "dispensary_name": "dispensary_name",
        "category": "type",
        "snapshot_time": "snapshot_time",
        "product_name": "product_name"
    },
    "iheartjane_zen_TGCONLY": {
        "price": "price",
        "brand": "brand",
        "quantity": "quantity",
        "dispensary_name": "store_name",
        "category": "type",
        "snapshot_time": "snapshot_time",
        "product_name": "product_name"
    }
}

# Fetch data from Firestore
def fetch_firestore_data(collection_name):
    """Fetch data from a Firestore collection."""
    docs = db.collection(collection_name).stream()
    data = [doc.to_dict() for doc in docs]
    return pd.DataFrame(data) if data else pd.DataFrame()

# Write data to Firestore
def write_to_firestore(data, collection_name):
    """Write data to a Firestore collection."""
    try:
        batch = db.batch()
        for i, row in data.iterrows():
            doc_ref = db.collection(collection_name).document()
            row_dict = row.to_dict()
            batch.set(doc_ref, {
                "price": row_dict.get("price", 0.0),
                "brand": row_dict.get("brand", "Unknown"),
                "quantity": row_dict.get("quantity", 0),
                "dispensary_name": row_dict.get("dispensary_name", "Unknown"),
                "category": row_dict.get("category", "Unknown"),
                "snapshot_time": row_dict.get("snapshot_time", datetime.now().isoformat()),
                "product_name": row_dict.get("product_name", "Unknown")
            })
            if (i + 1) % 500 == 0:  # Commit in batches of 500
                batch.commit()
                batch = db.batch()
        batch.commit()
        print(f"Uploaded {len(data)} records to {collection_name}.")
    except Exception as e:
        print(f"Error uploading to Firestore: {e}")

# Main processing function
def standardize_and_combine_collections():
    collections = list(FIELD_MAPPING.keys())
    dataframes = []

    for collection in collections:
        print(f"Fetching data from {collection}...")
        df = fetch_firestore_data(collection)
        print(f"Columns in {collection} after fetching: {df.columns.tolist()}")
        
        if not df.empty:
            print(f"Fetched {len(df)} records from {collection}.")
            
            # Rename columns based on FIELD_MAPPING
            mapping = FIELD_MAPPING[collection]
            for target, source in mapping.items():
                if source in df.columns:
                    df.rename(columns={source: target}, inplace=True)
                else:
                    print(f"Field {source} not found in {collection}. Filling {target} with default values.")
                    df[target] = "Unknown" if target in ["product_name", "brand", "dispensary_name", "category"] else 0

            # Filter only required columns
            df = df[["price", "brand", "quantity", "dispensary_name", "category", "snapshot_time", "product_name"]]

            # Append to dataframes
            dataframes.append(df)
        else:
            print(f"No data found in {collection}.")

    if not dataframes:
        print("No data to process.")
        return

    # Combine data
    combined_data = pd.concat(dataframes, ignore_index=True)
    print(f"Total records combined: {len(combined_data)}")

    # Fill missing values
    combined_data.fillna({
        "quantity": 0,
        "price": 0.0,
        "category": "Unknown",
        "brand": "Unknown",
        "dispensary_name": "Unknown",
        "product_name": "Unknown",
        "snapshot_time": datetime.now().isoformat()
    }, inplace=True)

    # Remove duplicates
    combined_data.drop_duplicates(
        subset=["snapshot_time", "dispensary_name", "product_name"],
        inplace=True
    )
    print(f"Records after deduplication: {len(combined_data)}")

    # Write combined data to Firestore
    write_to_firestore(combined_data, "TGC_Standardized")

if __name__ == "__main__":
    standardize_and_combine_collections()
