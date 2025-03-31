import os
from datetime import datetime
from firebase_admin import credentials, firestore, initialize_app, get_app

# Path to credentials
CREDENTIAL_PATH = os.path.join(
    os.path.dirname(__file__),
    "processors", ".secrets", "thegrowersresource.json"
)

# Initialize Firebase
try:
    get_app()
except ValueError:
    cred = credentials.Certificate(CREDENTIAL_PATH)
    initialize_app(cred)

db = firestore.client()

def delete_old_documents(collection_name="wallflower_only", cutoff_date="2025-03-25", batch_size=500):
    cutoff_datetime = datetime.strptime(cutoff_date, "%Y-%m-%d")
    total_deleted = 0

    while True:
        query = (
            db.collection(collection_name)
            .where("snapshot_time", "<", cutoff_datetime)
            .limit(batch_size)
        )

        docs = list(query.stream())
        if not docs:
            break

        batch = db.batch()
        for doc in docs:
            batch.delete(doc.reference)
        batch.commit()

        total_deleted += len(docs)
        print(f"🗑️ Deleted {total_deleted} documents so far...")

    print(f"✅ Finished deleting all documents before {cutoff_date}.")

if __name__ == "__main__":
    delete_old_documents()
