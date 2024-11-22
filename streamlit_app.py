import streamlit as st
from dutchie_processor import process_dutchie_data
from typesense_processor import process_typesense_data
from standardize_processor import process_standardized_inventory_data
from iheartjane_processor import process_iheartjane_data
from dutchieZen_processor import process_dutchiezen_data
from data_fetcher import fetch_inventory_data

# Set page layout
st.set_page_config(layout='wide')

# Initialize session state for all dataframes
if 'df_dutchie' not in st.session_state:
    st.session_state['df_dutchie'] = None

if 'df_iheartjane' not in st.session_state:
    st.session_state['df_iheartjane'] = None

if 'df_dutchieZen' not in st.session_state:
    st.session_state['df_dutchieZen'] = None

if 'df_typesense' not in st.session_state:
    st.session_state['df_typesense'] = None

if 'df_inventory' not in st.session_state:
    st.session_state['df_inventory'] = None

if 'df_inventory_filtered' not in st.session_state:
    st.session_state['df_inventory_filtered'] = None

# Main container for app content
main_container = st.container()

with main_container:
    # Navigation button to external resources
    st.markdown(
        f'<a href="https://docs.google.com/spreadsheets/d/1jHBz2zzvIO8KcWscrqq43wqV74DLiq1Jv1DjB2nd1DE/edit?usp=sharing" target="_blank">'
        f'<button style="margin: 2px; padding: 10px; background-color: #4CAF50; color: white;">Open Drops & Sell-Thru</button></a>',
        unsafe_allow_html=True,
    )

    # iHeartJane Data
    # if st.button("Fetch iHeartJane Data"):
    #     query = 'SELECT * FROM "public"."IheartJaneZen"'
    #     df_iheartjane = fetch_inventory_data(query)
    #     if df_iheartjane is not None:
    #         st.session_state['df_iheartjane'] = df_iheartjane

    # if st.session_state['df_iheartjane'] is not None:
    #     process_iheartjane_data()

    # # Dutchie Data
    # if st.button("Fetch Dutchie Data"):
    #     query = 'SELECT * FROM public.t_dynamic_inventory_reverseddates'
    #     df_dutchie = fetch_inventory_data(query)
    #     if df_dutchie is not None:
    #         st.session_state['df_dutchie'] = df_dutchie

    # if st.session_state['df_dutchie'] is not None:
    #     process_dutchie_data()

    # # Dutchie ZenRows Data
    # if st.button("Fetch Dutchie ZenRows Data"):
    #     query = 'SELECT * FROM public."dutchieZen"'
    #     df_dutchieZen = fetch_inventory_data(query)
    #     if df_dutchieZen is not None:
    #         st.session_state['df_dutchieZen'] = df_dutchieZen

    # if st.session_state['df_dutchieZen'] is not None:
    #     process_dutchiezen_data()

    # # Typesense Data
    # if st.button("Fetch Typesense Data"):
    #     query = 'SELECT * FROM public.typesense_table'
    #     df_typesense = fetch_inventory_data(query)
    #     if df_typesense is not None:
    #         st.session_state['df_typesense'] = df_typesense

    # if st.session_state['df_typesense'] is not None:
    #     process_typesense_data()

    # # Standardized Inventory Data
    # if st.button("Fetch Standardized Data"):
    #     query = 'SELECT * FROM public.standardized_inventory'
    #     df_inventory = fetch_inventory_data(query)
    #     if df_inventory is not None:
    #         st.session_state['df_inventory'] = df_inventory
    #         st.session_state['df_inventory_filtered'] = df_inventory

    # if st.session_state['df_inventory'] is not None:
    #     process_standardized_inventory_data()

    # Standardized Inventory with TGC Filtering
    if st.button("Fetch TGC from All Dispensaries"):
        query = """
            SELECT * FROM public.standardized_inventory
            WHERE LOWER(TRIM(brand)) IN (
                'grower circle', 
                'growers circle', 
                'grower circle apparel', 
                'the grower circle', 
                'flight bites', 
                'the grower circle'
            )
        """
        df_inventory_tgc = fetch_inventory_data(query)
        if df_inventory_tgc is not None:
            st.session_state['df_inventory'] = df_inventory_tgc
            st.session_state['df_inventory_filtered'] = df_inventory_tgc

    if st.session_state['df_inventory_filtered'] is not None:
        process_standardized_inventory_data()
