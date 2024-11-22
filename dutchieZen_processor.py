import streamlit as st
import pandas as pd
import plotly.express as px
from filters import update_filter_options_dutchie

pd.set_option("styler.render.max_elements", 5000000)

# Function to apply conditional formatting
def highlight_inventory(val):
    if pd.notna(val):
        if val < 10:
            return 'background-color: red'
        elif val < 15:
            return 'background-color: yellow'
    return ''

# Function to format inventory data as integers
def format_to_integer(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df

# Function to calculate sales
def calculate_sales(row, date_cols):
    reversed_date_cols = date_cols[::-1]
    diffs = row[reversed_date_cols].diff(-1).fillna(0)
    return diffs[diffs >= 0].sum()

# Main processor for dutchieZen data
def process_dutchiezen_data():
    # Check if data is in session state
    if 'df_dutchieZen' not in st.session_state:
        st.error("No data loaded for dutchieZen.")
        return

    # Retrieve data from session state
    df_dutchieZen = st.session_state['df_dutchieZen']

    # Ensure `snapshot_time` is in datetime format
    df_dutchieZen['snapshot_time'] = pd.to_datetime(df_dutchieZen['snapshot_time'], errors='coerce')

    # Pivot the data for easier display and analysis
    df_pivoted = df_dutchieZen.pivot_table(
        index=['dispensary_name', 'product_name', 'type', 'price', 'brand'],
        columns='snapshot_time',
        values='quantity',
        aggfunc='first'
    ).reset_index()

    # Identify recent date columns for filtering and calculations
    recent_dates = sorted([col for col in df_pivoted.columns if isinstance(col, pd.Timestamp)], reverse=True)

    if not recent_dates:
        st.error("No valid snapshot dates found. Please check the data.")
        return

    # Format the inventory numbers as integers
    df_pivoted = format_to_integer(df_pivoted, recent_dates)

    # Calculate sales since yesterday
    if len(recent_dates) >= 2:
        df_pivoted['Sales_Since_Yesterday'] = df_pivoted[recent_dates[1]] - df_pivoted[recent_dates[0]]
    else:
        df_pivoted['Sales_Since_Yesterday'] = "NA"

    # Calculate sales for the last 3, 7, and 30 days
    df_pivoted['Sales_Last_3_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, recent_dates[:3]), axis=1)
    df_pivoted['Sales_Last_7_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, recent_dates[:7]), axis=1)
    df_pivoted['Sales_Last_30_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, recent_dates[:30]), axis=1)

    # Apply filters with immediate reactivity
    filter_options = update_filter_options_dutchie(df_dutchieZen)

    selected_brand = st.selectbox("Filter by Brand", filter_options["brands"], key='brand_filter_dutchieZen')
    if selected_brand != "ALL":
        df_pivoted = df_pivoted[df_pivoted['brand'] == selected_brand]

    selected_dispensary = st.selectbox("Filter by Dispensary", filter_options["dispensaries"], key='dispensary_filter_dutchieZen')
    if selected_dispensary != "ALL":
        df_pivoted = df_pivoted[df_pivoted['dispensary_name'] == selected_dispensary]

    selected_type = st.selectbox("Filter by Type", filter_options["categories"], key='type_filter_dutchieZen')
    if selected_type != "ALL":
        df_pivoted = df_pivoted[df_pivoted['type'] == selected_type]

    search_term = st.text_input("Search by Product Name", key='search_filter_dutchieZen')
    if search_term:
        df_pivoted = df_pivoted[df_pivoted['product_name'].str.contains(search_term, case=False)]

    # Display data
    st.title("Dutchie ZenRows Inventory and Sales Data")
    columns_to_display = ['dispensary_name', 'product_name', 'type', 'price', 'brand'] + recent_dates
    styled_df = df_pivoted[columns_to_display].style.applymap(highlight_inventory, subset=recent_dates)
    st.dataframe(styled_df, use_container_width=True)

    # Sales performance sections
    col1, col2 = st.columns(2)

    with col1:
        st.header("Short Term Performance")
        top_sold_yesterday = df_pivoted[['product_name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
        st.subheader("Yesterday")
        st.dataframe(top_sold_yesterday, use_container_width=True)

    with col2:
        st.header("Long Term Performance")
        top_sold_30_days = df_pivoted[['product_name', 'Sales_Last_30_Days']].sort_values('Sales_Last_30_Days', ascending=False)
        st.subheader("Last 30 Days")
        st.dataframe(top_sold_30_days, use_container_width=True)

        # Pie chart for top brands
        top_brands = df_pivoted.groupby('product_name')['Sales_Since_Yesterday'].sum().nlargest(5)
        fig = px.pie(values=top_brands.values, names=top_brands.index, title='Top 5 Products by Sales Since Yesterday')
        st.plotly_chart(fig, use_container_width=True)
