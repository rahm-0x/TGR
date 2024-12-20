import pandas as pd
from firebase_admin import credentials, firestore, initialize_app
from datetime import datetime, date  # Import datetime and date for type checking

# Initialize Firebase Admin SDK
cred = credentials.Certificate("/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
initialize_app(cred)
db = firestore.client()

def fetch_firestore_data(collection_name, page_size=1000):
    """Fetch data from a Firestore collection in batches."""
    docs = []
    collection_ref = db.collection(collection_name)
    last_doc = None

    while True:
        query = collection_ref.order_by("__name__").limit(page_size)
        if last_doc:
            query = query.start_after(last_doc)

        batch_docs = query.stream()
        batch = list(batch_docs)
        if not batch:
            break

        docs.extend(batch)
        last_doc = batch[-1]

    # Convert documents to a DataFrame
    data = [doc.to_dict() for doc in docs]
    return pd.DataFrame(data) if data else pd.DataFrame()


def write_to_firestore(collection_name, data):
    """Write data to a Firestore collection."""
    batch = db.batch()
    for i, row in data.iterrows():
        doc_ref = db.collection(collection_name).document()
        # Convert datetime.date to datetime.datetime
        row_dict = row.to_dict()
        for key, value in row_dict.items():
            if isinstance(value, (pd.Timestamp, date)):  # Check for both Timestamp and date
                row_dict[key] = pd.Timestamp(value).to_pydatetime()  # Convert to datetime.datetime
        batch.set(doc_ref, row_dict)
        if (i + 1) % 500 == 0:  # Commit every 500 documents
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"{len(data)} records written to {collection_name}.")

def standardize_data():
    # Fetch data from Firestore collections
    df_iheartjane = fetch_firestore_data("iheartjane_zen")
    df_dutchiezen = fetch_firestore_data("dutchie_zen")
    df_typesense = fetch_firestore_data("typesense_table")

    # Map column names for Typesense table
    if not df_typesense.empty:
        df_typesense.rename(columns={
            "snapshot_time": "snapshot_time",
            "Location": "dispensary_name",
            "name": "product_name",  # Updated from Product_Name
            "option1Price": "price",  # Correct mapping for price
            "qty": "quantity",  # Updated from Available_Quantity
            "brand": "brand",
            "categoryName": "category"  # Updated to use categoryName
        }, inplace=True)

    # Standardize column names
    df_iheartjane.rename(columns={
        "snapshot_time": "snapshot_time",
        "store_name": "dispensary_name",
        "product_name": "product_name",
        "price": "price",
        "quantity": "quantity",
        "brand": "brand",
        "type": "category"
    }, inplace=True)

    df_dutchiezen.rename(columns={
        "snapshot_time": "snapshot_time",
        "dispensary_name": "dispensary_name",
        "product_name": "product_name",
        "price": "price",
        "quantity": "quantity",
        "brand": "brand",
        "type": "category"
    }, inplace=True)

    # Combine all data into one DataFrame
    df_combined = pd.concat([df_iheartjane, df_dutchiezen, df_typesense], ignore_index=True)

    # Convert snapshot_time to snapshot_date (day-level data)
    df_combined['snapshot_date'] = pd.to_datetime(df_combined['snapshot_time']).dt.date

    # Group by snapshot_date, dispensary_name, product_name, price, brand, and category
    df_grouped = df_combined.groupby(
        ['snapshot_date', 'dispensary_name', 'product_name', 'price', 'brand', 'category'], 
        as_index=False
    ).agg({'quantity': 'sum'})  # Sum quantities to avoid duplicates

    # Fetch existing records from Firestore
    existing_data = fetch_firestore_data("standardized_inventory")
    if not existing_data.empty:
        # Ensure the `snapshot_date` column is a `datetime.date` type
        existing_data['snapshot_date'] = pd.to_datetime(existing_data['snapshot_date']).dt.date
        existing_records = existing_data[['snapshot_date', 'dispensary_name', 'product_name']]
    else:
        existing_records = pd.DataFrame(columns=['snapshot_date', 'dispensary_name', 'product_name'])

    # Ensure `snapshot_date` column in `df_grouped` is also `datetime.date` type
    df_grouped['snapshot_date'] = pd.to_datetime(df_grouped['snapshot_date']).dt.date

    # Identify new records
    new_data = df_grouped.merge(existing_records, on=['snapshot_date', 'dispensary_name', 'product_name'], how='left', indicator=True)
    new_data = new_data[new_data['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not new_data.empty:
        # Write new data to Firestor
        write_to_firestore("standardized_inventory", new_data)
    else:
        print("No new data to insert.")

if __name__ == "__main__":
    standardize_data()
