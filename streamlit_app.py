import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px

# Add the 'firebase' directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "processors")))

from processors.standardized_processor import process_standardized_inventory_data

# Dynamic path for Firebase credentials
FIREBASE_CREDENTIALS_PATH = os.path.join(
    os.path.dirname(__file__), "processors", ".secrets", "thegrowersresource-1f2d7-firebase-adminsdk-hj18n-7101b02dc4.json"
)

# Verify Firebase credentials file exists
if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
    st.error(f"Firebase credentials file not found at {FIREBASE_CREDENTIALS_PATH}")
    raise FileNotFoundError(f"Firebase credentials file not found at {FIREBASE_CREDENTIALS_PATH}")

# Sample data for sales, products, dispensaries, and categories
sales_data = {
    "Date": pd.date_range(start="2024-11-01", end="2024-11-30"),
    "Product Name": ["Product A"] * 10 + ["Product B"] * 10 + ["Product C"] * 10,
    "Dispensary": ["Dispensary X"] * 5 + ["Dispensary Y"] * 5 + ["Dispensary Z"] * 10 + ["Dispensary W"] * 10,
    "Total Sales": [500 + i * 20 for i in range(30)],
}

products_data = {
    "Product Name": ["Product A", "Product B", "Product C", "Product D", "Product E"],
    "Sales": [1500, 1200, 1100, 900, 800],
}

dispensaries_data = {
    "Dispensary Name": ["Dispensary X", "Dispensary Y", "Dispensary Z", "Dispensary W", "Dispensary V"],
    "Sales": [2000, 1800, 1700, 1600, 1500],
}

category_data = {
    "Category": ["Category 1", "Category 2", "Category 3", "Category 4", "Category 5"],
    "Sales": [1000, 950, 850, 800, 750],
}

inventory_status_data = {
    "Product Name": ["Product A", "Product B", "Product C", "Product D", "Product E"],
    "Stock Level": [30, 45, 25, 10, 50],
    "Reorder Point": [20, 30, 20, 15, 40],
}

# Convert data to DataFrames
df_sales = pd.DataFrame(sales_data)
df_products = pd.DataFrame(products_data)
df_dispensaries = pd.DataFrame(dispensaries_data)
df_categories = pd.DataFrame(category_data)
df_inventory = pd.DataFrame(inventory_status_data)

# Streamlit layout
st.set_page_config(layout="wide")

# Navigation simulation using session state
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "home"

# Navigation sidebar
with st.sidebar:
    st.title("Navigation")
    if st.button("🏠 Home Overview"):
        st.session_state["current_page"] = "home"
    if st.button("📊 Inventory"):
        st.session_state["current_page"] = "inventory_tables"
    if st.button("🔎 Product Overview"):
        st.session_state["current_page"] = "product_overview"

# Page: Home Overview
if st.session_state["current_page"] == "home":
    st.title("Dashboard Overview")

    # Bar graph in the middle
    st.header("Total Sales - Day Over Day")
    fig = px.bar(df_sales, x="Date", y="Total Sales", title="Total Sales by Date")
    st.plotly_chart(fig, use_container_width=True)

    # Best performing sections below the bar graph
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

# Page: Inventory Tables
elif st.session_state["current_page"] == "inventory_tables":
    st.title("Inventory")
    process_standardized_inventory_data()

# Page: Product Overview
elif st.session_state["current_page"] == "product_overview":
    st.title("Product Overview")

    # Filters: Product Name and Dispensary
    st.markdown("### Filter Options")
    col1, col2, col3 = st.columns([3, 3, 1])

    with col1:
        selected_products = st.multiselect(
            "Select Products",
            options=["All"] + list(df_sales["Product Name"].unique()),
            default=["All"],
            help="Select one or more products to filter"
        )
        if "All" in selected_products:
            selected_products = list(df_sales["Product Name"].unique())

    with col2:
        selected_dispensaries = st.multiselect(
            "Select Dispensaries",
            options=["All"] + list(df_sales["Dispensary"].unique()),
            default=["All"],
            help="Select one or more dispensaries to filter"
        )
        if "All" in selected_dispensaries:
            selected_dispensaries = list(df_sales["Dispensary"].unique())

    # Filter the data based on selections
    filtered_data = df_sales[
        (df_sales["Product Name"].isin(selected_products)) &
        (df_sales["Dispensary"].isin(selected_dispensaries))
    ]

    # Bar graph in the middle
    st.header("Total Sales - Day Over Day")
    fig = px.bar(
        filtered_data,
        x="Date",
        y="Total Sales",
        color="Dispensary",
        title=f"Total Sales for Selected Products and Dispensaries",
        labels={"Total Sales": "Sales ($)", "Date": "Date"}
    )
    st.plotly_chart(fig, use_container_width=True)

    # Insights below the graph
    st.subheader("Insights")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Performing Dispensaries")
        top_dispensaries = (
            filtered_data.groupby("Dispensary")["Total Sales"].sum().reset_index().sort_values(by="Total Sales", ascending=False)
        )
        st.table(top_dispensaries)

    with col2:
        st.subheader("Inventory Status")
        st.table(df_inventory)
