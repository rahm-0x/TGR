import os
from datetime import datetime
from firebase_admin import credentials, firestore, initialize_app, get_app

# Firebase credentials path
FIREBASE_CREDENTIALS_PATH = "/Users/phoenix/Desktop/TGR-Firebase/TGR/processors/.secrets/thegrowersresource.json"

# Initialize Firebase if not already
try:
    app = get_app()
except ValueError:
    cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
    initialize_app(cred)

db = firestore.client()

def copy_wallflower_documents_from_march_26():
    source = db.collection("TGC_Standardized")
    target = db.collection("wallflower_only")

    start_date = datetime(2025, 3, 26)

    wallflower_docs = source.stream()
    batch = db.batch()
    count = 0

    for doc in wallflower_docs:
        data = doc.to_dict()
        if (
            data.get("dispensary_name", "").lower() == "wallflower"
            and isinstance(data.get("snapshot_time"), datetime)
            and data["snapshot_time"] >= start_date
        ):
            batch.set(target.document(), data)
            count += 1
            if count % 500 == 0:
                batch.commit()
                batch = db.batch()

    batch.commit()
    print(f"✅ Successfully copied {count} documents from March 26 onward to 'wallflower_only'.")

if __name__ == "__main__":
    copy_wallflower_documents_from_march_26()
