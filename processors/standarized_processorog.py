import streamlit as st
import pandas as pd
import time
from datetime import datetime, date
from firebase_admin import credentials, firestore, initialize_app, get_app
import json

# Load Firebase credentials from Streamlit secrets
try:
    firebase_app = get_app()
except ValueError:
    firebase_config = dict(st.secrets["firebase"])
    cred = credentials.Certificate(firebase_config)
    firebase_app = initialize_app(cred)

db = firestore.client()

def fetch_firestore_data_paginated(collection_name, page_size=100):
    documents = []
    last_doc = None
    while True:
        query = db.collection(collection_name).order_by("snapshot_time", direction=firestore.Query.DESCENDING).limit(page_size)
        if last_doc:
            query = query.start_after(last_doc)
        current_docs = list(query.stream())
        batch = [doc.to_dict() for doc in current_docs]
        if not batch:
            break
        documents.extend(batch)
        last_doc = current_docs[-1] if len(current_docs) == page_size else None
        if len(documents) >= 5000:
            break
    return pd.DataFrame(documents) if documents else pd.DataFrame()

def fetch_firestore_data_with_retries(collection_name, retries=5, delay=1):
    for attempt in range(retries):
        try:
            return fetch_firestore_data_paginated(collection_name)
        except Exception as e:
            st.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                st.error(f"All retries failed for {collection_name}.")
                raise e

def highlight_inventory(val):
    try:
        val = float(val)
        if val < 0:
            return ''
        if val < 15:
            return 'background-color: red'
        elif val < 25:
            return 'background-color: yellow'
    except:
        return ''
    return ''

def format_to_integer(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df

def process_standardized_inventory_data():
    df_inventory = fetch_firestore_data_with_retries("TGC_Standardized")
    if df_inventory.empty:
        st.error("No data loaded for Standardized Inventory.")
        return

    tgc_brands = [
        "grower circle", "growers circle", "grower circle apparel",
        "the grower circle", "flight bites", "the grower circle"
    ]
    df_inventory = df_inventory[df_inventory['brand'].str.lower().str.strip().isin(tgc_brands)]

    df_inventory['snapshot_time'] = pd.to_datetime(df_inventory['snapshot_time'], errors='coerce')
    df_inventory.dropna(subset=['snapshot_time'], inplace=True)
    df_inventory = df_inventory[df_inventory['snapshot_time'].dt.date >= date(2025, 3, 25)]
    df_inventory['snapshot_date_str'] = df_inventory['snapshot_time'].dt.strftime('%Y-%m-%d')
    df_inventory['quantity'] = pd.to_numeric(df_inventory['quantity'], errors='coerce').fillna(0)

    df_inventory.sort_values(by=['dispensary_name', 'product_name', 'snapshot_time'], ascending=[True, True, False], inplace=True)
    df_inventory = df_inventory.drop_duplicates(subset=['dispensary_name', 'product_name', 'snapshot_date_str'], keep='first')

    df_grouped = df_inventory.groupby([
        'dispensary_name', 'product_name', 'price', 'brand', 'category', 'snapshot_date_str'
    ]).agg({'quantity': 'last'}).reset_index()

    df_pivoted = df_grouped.pivot(
        index=['dispensary_name', 'product_name', 'price', 'brand', 'category'],
        columns='snapshot_date_str',
        values='quantity'
    ).reset_index()

    df_pivoted.fillna(0, inplace=True)

    date_columns = [col for col in df_pivoted.columns if isinstance(col, str) and '-' in col]
    date_columns = sorted(date_columns, reverse=True)
    df_pivoted = format_to_integer(df_pivoted, date_columns)

    unique_categories = df_pivoted['category'].dropna().unique().tolist()
    unique_categories.insert(0, "ALL")
    selected_category = st.selectbox("Filter by Category", unique_categories, index=0, key="selected_category")
    if selected_category != "ALL":
        df_pivoted = df_pivoted[df_pivoted['category'] == selected_category]

    unique_dispensaries = df_pivoted['dispensary_name'].dropna().unique().tolist()
    unique_dispensaries.insert(0, "ALL")
    selected_dispensary = st.selectbox("Filter by Dispensary", unique_dispensaries, index=0, key="selected_dispensary")
    if selected_dispensary != "ALL":
        df_pivoted = df_pivoted[df_pivoted['dispensary_name'] == selected_dispensary]

    search_term = st.text_input("Search by Product Name", key="search_product")
    if search_term:
        df_pivoted = df_pivoted[df_pivoted['product_name'].str.contains(search_term, case=False)]

    st.title("Standardized Inventory and Sales Data")
    columns_to_display = ['dispensary_name', 'product_name', 'price', 'brand', 'category'] + date_columns
    styled_df = df_pivoted[columns_to_display].style.applymap(highlight_inventory, subset=date_columns)
    st.dataframe(styled_df)

    if len(date_columns) >= 2:
        df_pivoted['Sales_Since_Yesterday'] = df_pivoted[date_columns[0]] - df_pivoted[date_columns[1]]
        df_pivoted['Sales_Since_Yesterday'] = df_pivoted['Sales_Since_Yesterday'].astype(int)
    else:
        df_pivoted['Sales_Since_Yesterday'] = "NA"

    if len(date_columns) >= 4:
        df_pivoted['Sales_Last_3_Days'] = df_pivoted[date_columns[0]] - df_pivoted[date_columns[3]]
        df_pivoted['Sales_Last_3_Days'] = df_pivoted['Sales_Last_3_Days'].astype(int)
    else:
        df_pivoted['Sales_Last_3_Days'] = "NA"

    if len(date_columns) >= 7:
        df_pivoted['Sales_Last_7_Days'] = df_pivoted[date_columns[0]] - df_pivoted[date_columns[7]]
        df_pivoted['Sales_Last_7_Days'] = df_pivoted['Sales_Last_7_Days'].astype(int)
    else:
        df_pivoted['Sales_Last_7_Days'] = "NA"

    col1, col2 = st.columns(2)
    with col1:
        st.header("Short Term Performance")
        top_sold_yesterday = df_pivoted[['product_name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
        st.subheader("Yesterday")
        st.dataframe(top_sold_yesterday)

        top_sold_3_days = df_pivoted[['product_name', 'Sales_Last_3_Days']].sort_values('Sales_Last_3_Days', ascending=False)
        st.subheader("Last 3 Days")
        st.dataframe(top_sold_3_days)

    with col2:
        st.header("Long Term Performance")
        top_sold_7_days = df_pivoted[['product_name', 'Sales_Last_7_Days']].sort_values('Sales_Last_7_Days', ascending=False)
        st.subheader("Last 7 Days")
        st.dataframe(top_sold_7_days)

if __name__ == "__main__":
    process_standardized_inventory_data()
