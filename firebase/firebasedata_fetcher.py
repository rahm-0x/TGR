import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
if not firebase_admin._apps:
    cred = credentials.Certificate("/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
    firebase_admin.initialize_app(cred)

# Get Firestore client
db = firestore.client()

# Function to fetch data from Firestore
def fetch_inventory_data(collection_name):
    try:
        # Fetch all documents from the specified collection
        docs = db.collection(collection_name).stream()
        data = [doc.to_dict() for doc in docs]  # Convert Firestore documents to dictionaries
        if not data:
            st.warning(f"No data found in collection: {collection_name}")
            return pd.DataFrame()  # Return an empty DataFrame if no data is found
        
        # Convert the list of dictionaries to a pandas DataFrame
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Error fetching data from Firestore: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

# Example usage
if __name__ == "__main__":
    st.title("Inventory Data from Firestore")
    
    # Fetch data from a Firestore collection
    collection_name = "typesense_table"  # Example collection name
    inventory_data = fetch_inventory_data(collection_name)
    
    if not inventory_data.empty:
        st.dataframe(inventory_data)  # Display the data in Streamlit
