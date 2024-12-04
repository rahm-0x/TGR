import streamlit as st
from processors.standardized_processor import process_standardized_inventory_data

# Set page layout
st.set_page_config(layout="wide")

# Initialize session state for all dataframes
if "df_inventory_filtered" not in st.session_state:
    st.session_state["df_inventory_filtered"] = None

# Main container for app content
main_container = st.container()

with main_container:
    # Navigation button to external resources
    st.markdown(
        f'<a href="https://docs.google.com/spreadsheets/d/1jHBz2zzvIO8KcWscrqq43wqV74DLiq1Jv1DjB2nd1DE/edit?usp=sharing" target="_blank">'
        f'<button style="margin: 2px; padding: 10px; background-color: #4CAF50; color: white;">Open Drops & Sell-Thru</button></a>',
        unsafe_allow_html=True,
    )

    # Standardized Inventory with TGC Filtering
    if st.button("Fetch TGC from All Dispensaries"):
        # Process and fetch data using the standardized processor
        process_standardized_inventory_data()

        # Check if the filtered data exists in session state
        if st.session_state["df_inventory_filtered"] is not None:
            st.success(f"Filtered {len(st.session_state['df_inventory_filtered'])} records for TGC brands.")
        else:
            st.warning("No records found for the specified TGC brands.")

    # Display filtered data
    if st.session_state["df_inventory_filtered"] is not None:
        st.title("TGC Standardized Inventory Data")
        st.dataframe(st.session_state["df_inventory_filtered"], use_container_width=True)
    else:
        st.info("Click 'Fetch TGC from All Dispensaries' to load data.")
