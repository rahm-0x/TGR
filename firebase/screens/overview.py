import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px

# Add the 'firebase' directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from processors.standardized_processor import process_standardized_inventory_data

# Sample data for sales, products, dispensaries, and categories
sales_data = {
    "Date": pd.date_range(start="2024-11-01", end="2024-11-30"),
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

# Convert data to DataFrames
df_sales = pd.DataFrame(sales_data)
df_products = pd.DataFrame(products_data)
df_dispensaries = pd.DataFrame(dispensaries_data)
df_categories = pd.DataFrame(category_data)

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

    # Process standardized inventory data
    process_standardized_inventory_data()
