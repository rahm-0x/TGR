import pandas as pd
from firebase_admin import credentials, firestore, initialize_app
from datetime import datetime, date

# Initialize Firebase Admin SDK
cred = credentials.Certificate("/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
initialize_app(cred)
db = firestore.client()

# Grower Circle Brands (Standardized Capitalization)
GROWER_CIRCLE_BRANDS = {
    "grower circle": "The Grower Circle",
    "growers circle": "The Grower Circle",
    "grower circle apparel": "The Grower Circle",
    "the grower circle": "The Grower Circle",
    "flight bites": "Flight Bites"
}

# Category standardization mappings
CATEGORY_MAPPING = {
    "Edible": ["Edible", "edible", "EDIBLE - GUMMY", "EDIBLES", "Edibles", "THIRD PARTY EDIBLES - GENERAL"],
    "Pre-Rolls": [
        "1g Pre-Rolls", "pre-roll", "PRE-ROLL", "Pre-Rolls", "Infused Pre-Rolls", "Infused Pre-Rolls Solventless",
        "PREROLL - INFUSED", "Preroll", "Preroll Pack", "THIRD PARTY INFUSED PRE-ROLLS", "THIRD PARTY PRE-ROLLS"
    ],
    "Flower": [
        "Flower", "flower", "FLOWER", "FLOWER - PRE-PACK", "THIRD PARTY FLOWER - GENERAL", "Flower 54"
    ],
    "Concentrate": ["Concentrate", "RSO"],
    "Vape": ["vape", "Vape", "CARTRIDGE - 510 THREAD", "Solventless Cartridges", "Solventless Disposables", "Vaporizers"],
}

# Reverse category mapping for lookup
REVERSE_CATEGORY_MAPPING = {value: key for key, values in CATEGORY_MAPPING.items() for value in values}

# Fetch data from Firestore
def fetch_firestore_data(collection_name):
    """Fetch data from a Firestore collection."""
    docs = db.collection(collection_name).stream()
    data = [doc.to_dict() for doc in docs]
    return pd.DataFrame(data) if data else pd.DataFrame()

# Write data to Firestore
def write_to_firestore(collection_name, data):
    """Write data to a Firestore collection."""
    batch = db.batch()
    for i, row in data.iterrows():
        doc_ref = db.collection(collection_name).document()
        row_dict = row.to_dict()
        for key, value in row_dict.items():
            if isinstance(value, (pd.Timestamp, date)):
                row_dict[key] = pd.Timestamp(value).to_pydatetime()
        batch.set(doc_ref, row_dict)
        if (i + 1) % 500 == 0:  # Commit every 500 documents
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"{len(data)} records written to {collection_name}.")

# Standardize categories
def standardize_category(category):
    """Map a category to its standardized form."""
    return REVERSE_CATEGORY_MAPPING.get(category, category)

# Standardize brand names
def standardize_brand(brand):
    """Map a brand to its standardized capitalization."""
    return GROWER_CIRCLE_BRANDS.get(brand.lower(), brand)

# Main standardization script
def process_and_standardize_data():
    # Fetch data from all sources
    df_iheartjane = fetch_firestore_data("iheartjane_zen_TGCONLY")
    df_dutchiezen = fetch_firestore_data("dutchie_zen_TGCONLY")
    df_typesense = fetch_firestore_data("typesense_table_TGCONLY")
    
    print(f"Fetched {len(df_iheartjane)} records from iheartjane_zen_TGCONLY.")
    print(f"Fetched {len(df_dutchiezen)} records from dutchie_zen_TGCONLY.")
    print(f"Fetched {len(df_typesense)} records from typesense_table_TGCONLY.")

    print("Typesense raw data:")
    print(df_typesense[['Location']].drop_duplicates())

    # Standardize columns
    column_mapping = {
        "snapshot_time": "snapshot_time",
        "store_name": "dispensary_name",
        "Location": "dispensary_name",
        "product_name": "product_name",
        "name": "product_name",
        "price": "price",
        "bucket_price": "price",
        "quantity": "quantity",
        "Available_Quantity": "quantity",
        "max_cart_quantity": "quantity",
        "brand": "brand",
        "Brand": "brand",
        "type": "category",
        "categoryName": "category"
    }
    dfs = [df_iheartjane, df_dutchiezen, df_typesense]
    standardized_dfs = []
    for df in dfs:
        if not df.empty:
            df.rename(columns=column_mapping, inplace=True)
            standardized_dfs.append(df)

    # Combine all data
    df_combined = pd.concat(standardized_dfs, ignore_index=True)
    print("Unique dispensary names in combined data:")
    print(df_combined['dispensary_name'].dropna().unique())

    # Standardize brand and dispensary names
    df_combined['brand'] = df_combined['brand'].apply(standardize_brand)
    df_combined['dispensary_name'] = df_combined['dispensary_name'].str.strip().str.title()

    # Log after standardization
    print("Unique dispensary names after standardization:")
    print(df_combined['dispensary_name'].dropna().unique())

    # Convert snapshot_time to snapshot_date
    df_combined['snapshot_date'] = pd.to_datetime(df_combined['snapshot_time'], errors='coerce').dt.date

    # Standardize categories
    df_combined['category'] = df_combined['category'].apply(standardize_category)

    # Group by key columns
    df_grouped = df_combined.groupby(
        ['snapshot_date', 'dispensary_name', 'product_name', 'price', 'brand', 'category'],
        as_index=False
    ).agg({'quantity': 'sum'})  # Aggregate quantity

    print("Grouped data:")
    print(df_grouped.head())

    # Fetch existing records from Firestore
    existing_data = fetch_firestore_data("standardized_inventory_cleaned")
    print(f"Fetched {len(existing_data)} records from standardized_inventory_cleaned.")
    if not existing_data.empty:
        existing_data['snapshot_date'] = pd.to_datetime(existing_data['snapshot_date'], errors='coerce').dt.date
        existing_records = existing_data[['snapshot_date', 'dispensary_name', 'product_name']]
    else:
        existing_records = pd.DataFrame(columns=['snapshot_date', 'dispensary_name', 'product_name'])

    # Find new records
    df_grouped['snapshot_date'] = pd.to_datetime(df_grouped['snapshot_date'], errors='coerce').dt.date
    new_data = df_grouped.merge(existing_records, on=['snapshot_date', 'dispensary_name', 'product_name'], how='left', indicator=True)
    new_data = new_data[new_data['_merge'] == 'left_only'].drop(columns=['_merge'])

    print("New data to be written:")
    print(new_data.head())

    # Write new data to Firestore
    if not new_data.empty:
        write_to_firestore("standardized_inventory_cleaned", new_data)
    else:
        print("No new data to insert.")

if __name__ == "__main__":
    process_and_standardize_data()
