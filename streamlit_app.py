import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from firebase_admin import credentials, firestore, initialize_app, get_app

# Streamlit layout config must be the first Streamlit command
st.set_page_config(layout="wide")

# Add the 'firebase' directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "processors")))

from processors.standarized_processorog import process_standardized_inventory_data

# Firebase credentials
FIREBASE_CREDENTIALS_PATH = os.path.join(
    os.path.dirname(__file__), "processors", ".secrets", "thegrowersresource.json"
)

if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
    st.error(f"Firebase credentials file not found at {FIREBASE_CREDENTIALS_PATH}")
    raise FileNotFoundError(f"Firebase credentials file not found at {FIREBASE_CREDENTIALS_PATH}")

# Initialize Firebase only if not already initialized
try:
    firebase_app = get_app()
except ValueError:
    cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
    firebase_app = initialize_app(cred)

db = firestore.client()

# Fetch sales data from Firestore from wallflower_only collection
def get_wallflower_data():
    docs = db.collection("wallflower_only").stream()
    rows = [doc.to_dict() for doc in docs]
    return pd.DataFrame(rows)

# Pull actual data
wallflower_df = get_wallflower_data()

if wallflower_df.empty:
    st.error("No data found for Wallflower.")
else:
    wallflower_df["snapshot_time"] = pd.to_datetime(wallflower_df["snapshot_time"], errors="coerce")
    wallflower_df.dropna(subset=["snapshot_time"], inplace=True)

    # Page State
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "home"

    # Sidebar Navigation
    with st.sidebar:
        st.title("Navigation")
        if st.button("\U0001F3E0 Home Overview"):
            st.session_state["current_page"] = "home"
        if st.button("\U0001F4CA Inventory"):
            st.session_state["current_page"] = "inventory_tables"
        if st.button("\U0001F50E Product Overview"):
            st.session_state["current_page"] = "product_overview"

    if st.session_state["current_page"] != "inventory_tables":
        # Top-level dropdown filters
        col_filter_left, col_filter_right = st.columns([6, 1])

        with col_filter_left:
            all_dispensaries = wallflower_df["dispensary_name"].dropna().unique().tolist()
            selected_dispensary = st.selectbox("Select Dispensary:", all_dispensaries)

        with col_filter_right:
            latest_date = wallflower_df["snapshot_time"].dt.date.max()
            recent_dates = sorted(wallflower_df["snapshot_time"].dt.date.unique())
            filtered_dates = [str(d) for d in recent_dates if pd.to_datetime("2025-03-26").date() <= d <= latest_date]
            date_choice = st.selectbox("Select Date:", filtered_dates)

        # Filter by dispensary
        wallflower_df = wallflower_df[wallflower_df["dispensary_name"] == selected_dispensary]

    df_sales = pd.DataFrame()
    df_products = pd.DataFrame()
    df_dispensaries = pd.DataFrame()
    df_categories = pd.DataFrame()
    df_inventory = pd.DataFrame()

    if st.session_state["current_page"] != "inventory_tables":
        selected_date = pd.to_datetime(date_choice).date()
        prev_date = selected_date - pd.Timedelta(days=1)
        df_before = wallflower_df[wallflower_df["snapshot_time"].dt.date == prev_date]
        df_selected = wallflower_df[wallflower_df["snapshot_time"].dt.date == selected_date]

        df_before_sorted = df_before.sort_values("snapshot_time").drop_duplicates(subset="product_name", keep="last")
        df_selected_sorted = df_selected.sort_values("snapshot_time").drop_duplicates(subset="product_name", keep="last")

        df_merged = pd.merge(df_before_sorted, df_selected_sorted, on="product_name", suffixes=("_before", "_after"))
        df_merged["sold"] = df_merged["quantity_before"] - df_merged["quantity_after"]
        df_merged["sold"] = df_merged["sold"].clip(lower=0)

        df_sales = df_merged[["product_name", "sold"]].copy()
        df_sales.columns = ["Product Name", "Total Sales"]
        df_sales["Date"] = selected_date

        df_products = df_sales.groupby("Product Name")["Total Sales"].sum().reset_index().sort_values(by="Total Sales", ascending=False).head(5)
        df_dispensaries = pd.DataFrame({"Dispensary Name": [selected_dispensary], "Sales": [df_sales["Total Sales"].sum()]})

        if "category_after" in df_merged.columns:
            categories = df_merged[["product_name", "category_after", "sold"]].copy()
            categories.columns = ["Product Name", "Category", "Sales"]
            df_categories = categories.groupby("Category")["Sales"].sum().reset_index().sort_values(by="Sales", ascending=False).head(5)

        df_inventory = df_selected_sorted[["product_name", "quantity"]].copy()
        df_inventory.columns = ["Product Name", "Stock Level"]
        df_inventory["Reorder Point"] = 25

    # Page: Home Overview
    if st.session_state["current_page"] == "home":
        st.title("Dashboard Overview")

        st.header("Total Sales - Top Performing Products")
        if not df_products.empty:
            fig = px.bar(
                df_products.sort_values("Total Sales", ascending=False),
                x="Product Name",
                y="Total Sales",
                title=f"{selected_dispensary}: Top Performing Products"
            )
            st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader("Top Performing Products")
            st.table(df_products)

        with col3:
            st.subheader("Top Performing Dispensaries")
            st.table(df_dispensaries)

        with col1:
            st.subheader("Best Performing by Category")
            st.table(df_categories)

    elif st.session_state["current_page"] == "inventory_tables":
        st.title("Inventory")
        process_standardized_inventory_data()

    elif st.session_state["current_page"] == "product_overview":
        st.title("Product Overview")

        st.markdown("### Filter Options")
        col1, col2, col3 = st.columns([3, 3, 1])

        with col1:
            selected_products = st.multiselect(
                "Select Products",
                options=["All"] + list(df_sales["Product Name"].unique()),
                default=["All"]
            )
            if "All" in selected_products:
                selected_products = list(df_sales["Product Name"].unique())

        with col2:
            selected_dispensaries = [selected_dispensary]

        filtered_data = df_sales[
            (df_sales["Product Name"].isin(selected_products))
        ]

        st.subheader("Insights")
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top Performing Products")
            top_products = (
                filtered_data.groupby("Product Name")["Total Sales"].sum().reset_index().sort_values(by="Total Sales", ascending=False)
            )
            st.table(top_products)

        with col2:
            st.subheader("Inventory Status")
            st.table(df_inventory)
