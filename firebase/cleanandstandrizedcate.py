import pandas as pd
from firebase_admin import credentials, firestore, initialize_app

# Initialize Firebase Admin SDK
cred = credentials.Certificate("/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
initialize_app(cred)
db = firestore.client()

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
        batch.set(doc_ref, row.to_dict())
        if (i + 1) % 500 == 0:  # Commit every 500 documents
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"{len(data)} records written to {collection_name}.")

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
    # Add other mappings as needed
}

# Reverse the CATEGORY_MAPPING for lookup
REVERSE_CATEGORY_MAPPING = {
    value: key for key, values in CATEGORY_MAPPING.items() for value in values
}

# Standardize categories
def standardize_category(category):
    """Map a category to its standardized form."""
    return REVERSE_CATEGORY_MAPPING.get(category, category)

def clean_and_standardize_categories():
    # Fetch data from the standardized_inventory collection
    df = fetch_firestore_data("standardized_inventory")

    if df.empty:
        print("No data found in standardized_inventory.")
        return

    # Standardize the category column
    df['category'] = df['category'].apply(standardize_category)

    # Write the cleaned data back to Firestore
    write_to_firestore("standardized_inventory_cleaned", df)

    print("Categories have been standardized and saved to standardized_inventory_cleaned.")

if __name__ == "__main__":
    clean_and_standardize_categories()
