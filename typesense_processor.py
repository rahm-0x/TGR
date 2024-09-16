import streamlit as st
import pandas as pd
import plotly.express as px
from filters import update_filter_options_typesense

# Calculate sales for a range of dates
def calculate_sales(row, date_cols):
    reversed_date_cols = date_cols[::-1]
    diffs = row[reversed_date_cols].diff(-1).fillna(0).infer_objects()
    return diffs[diffs >= 0].sum()

def process_typesense_data():
    if 'df_typesense' in st.session_state:
        df_typesense = st.session_state['df_typesense']
        df_typesense['snapshot_time'] = pd.to_datetime(df_typesense['snapshot_time'])

        # Apply filters
        filter_options_typesense = update_filter_options_typesense(df_typesense)
        selected_brand_typesense = st.selectbox("Filter by Brand (Typesense)", filter_options_typesense["brands"], key='brand_filter_typesense')
        if selected_brand_typesense != "ALL":
            df_typesense = df_typesense[df_typesense['Brand'] == selected_brand_typesense]

        selected_location_typesense = st.selectbox("Filter by Location (Typesense)", filter_options_typesense["locations"], key='location_filter_typesense')
        if selected_location_typesense != "ALL":
            df_typesense = df_typesense[df_typesense['Location'] == selected_location_typesense]

        selected_category_typesense = st.selectbox("Filter by Category (Typesense)", filter_options_typesense["categories"], key='category_filter_typesense')
        if selected_category_typesense != "ALL":
            df_typesense = df_typesense[df_typesense['Category'] == selected_category_typesense]

        search_term_typesense = st.text_input("Search (Typesense)", key='typesense_search')
        if search_term_typesense:
            df_typesense = df_typesense[df_typesense.apply(lambda row: row.astype(str).str.contains(search_term_typesense, case=False).any(), axis=1)]

        # Pivot data for snapshot columns
        df_pivoted = df_typesense.pivot_table(
            index=['Location', 'Product_Name', 'Price', 'Brand', 'Category'],
            columns='snapshot_time',
            values='Available_Quantity',
            aggfunc='first'
        ).reset_index()

        st.title("Typesense Inventory")
        st.dataframe(df_pivoted)

        # Dynamically find the most recent date columns (snapshot dates)
        recent_dates = sorted([col for col in df_pivoted.columns if isinstance(col, pd.Timestamp)], reverse=True)

        if len(recent_dates) >= 2:
            df_pivoted['Sales_Since_Yesterday'] = df_pivoted[recent_dates[1]] - df_pivoted[recent_dates[0]]
        else:
            df_pivoted['Sales_Since_Yesterday'] = "NA"

        df_pivoted['Sales_Since_Yesterday'] = pd.to_numeric(df_pivoted['Sales_Since_Yesterday'], errors='coerce')

        last_3_days = recent_dates[:3]
        last_7_days = recent_dates[:7]
        last_30_days = recent_dates[:30]

        df_pivoted['Sales_Last_3_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, last_3_days), axis=1)
        df_pivoted['Sales_Last_7_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, last_7_days), axis=1)
        df_pivoted['Sales_Last_30_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, last_30_days), axis=1)

        # Filter for Out of Stock products (Available_Quantity == 0 or None)
        out_of_stock = df_pivoted[
            (df_pivoted[recent_dates[0]].isna()) | 
            (df_pivoted[recent_dates[0]] == 0)
        ][['Location', 'Product_Name', 'Category', 'Price', recent_dates[0]]]

        # Display sales data and pie chart
        col1, col2 = st.columns(2)

        with col1:
            st.header("Short Term Performance")
            top_sold_yesterday = df_pivoted[['Product_Name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
            top_sold_3_days = df_pivoted[['Product_Name', 'Sales_Last_3_Days']].sort_values('Sales_Last_3_Days', ascending=False)
            
            # Update out-of-stock products filtering
            st.subheader("Yesterday")
            st.dataframe(top_sold_yesterday)
            st.subheader("Last 3 Days")
            st.dataframe(top_sold_3_days)
            st.header("Out of Stock Products")
            st.dataframe(out_of_stock.rename(columns={recent_dates[0]: 'Available Quantity'}))

        with col2:
            st.header("Long Term Performance")
            top_sold_7_days = df_pivoted[['Product_Name', 'Sales_Last_7_Days']].sort_values('Sales_Last_7_Days', ascending=False)
            top_sold_30_days = df_pivoted[['Product_Name', 'Sales_Last_30_Days']].sort_values('Sales_Last_30_Days', ascending=False)

            st.subheader("Last 7 Days")
            st.dataframe(top_sold_7_days)
            st.subheader("Last 30 Days")
            st.dataframe(top_sold_30_days)

            # Pie chart for top products by sales since yesterday
            top_brands = df_pivoted.groupby('Product_Name')['Sales_Since_Yesterday'].sum().nlargest(5)
            fig = px.pie(values=top_brands.values, names=top_brands.index, title='Top 5 Products by Sales Since Yesterday', labels={'value': 'Sales Since Yesterday'})
            fig.update_traces(textinfo='label+value')
            st.plotly_chart(fig, use_container_width=True)
