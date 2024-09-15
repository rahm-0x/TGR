import streamlit as st
from dutchie_processor import process_dutchie_data
from typesense_processor import process_typesense_data
from data_fetcher import fetch_inventory_data

# Set page layout
st.set_page_config(layout='wide')

main_container = st.container()

with main_container:
    st.markdown(
        f'<a href="https://docs.google.com/spreadsheets/d/1jHBz2zzvIO8KcWscrqq43wqV74DLiq1Jv1DjB2nd1DE/edit?usp=sharing" target="_blank">'
        f'<button style="margin: 2px; padding: 10px; background-color: #4CAF50; color: white;">Open Drops & Sell-Thru</button></a>',
        unsafe_allow_html=True,
    )

    # Button to fetch Dutchie data
    if st.button("Fetch Dutchie Data"):
        query = 'SELECT * FROM public.t_dynamic_inventory_reverseddates'
        df_dutchie = fetch_inventory_data(query)
        st.session_state['df_dutchie'] = df_dutchie

    # Button to fetch TGC Dutchie data
    elif st.button("Fetch TGC (Dutchie)"):
        query = "SELECT * FROM public.t_dynamic_inventory_reverseddates WHERE brand LIKE 'The Grower Circle'"
        df_dutchie = fetch_inventory_data(query)
        st.session_state['df_dutchie'] = df_dutchie

    # Process Dutchie data
    process_dutchie_data()

    # Button to fetch Typesense data
    if st.button("Fetch Typesense Data"):
        query = 'SELECT * FROM public.typesense_table'
        df_typesense = fetch_inventory_data(query)
        st.session_state['df_typesense'] = df_typesense


    if st.button("Fetch TGC from Typesense"):
        query = """
            SELECT * FROM public.typesense_table
            WHERE "Brand" ILIKE 'Grower Circle'
            OR "Brand" ILIKE 'GROWERS CIRCLE'
            OR "Brand" ILIKE 'Grower Circle Apparel'
            OR "Brand" ILIKE 'THE GROWER CIRCLE'
            OR "Brand" ILIKE 'Flight Bites'
            OR "Brand" ILIKE 'FLIGHT BITES'
            OR "Brand" ILIKE 'The Grower Circle'
        """
        df_typesense_tgc = fetch_inventory_data(query)
        st.session_state['df_typesense'] = df_typesense_tgc


    # if st.button("Fetch Curaleaf Data"):
    #     query = 'SELECT * FROM curaleaf_data'
    #     df_curaleaf = fetch_inventory_data(query)
    #     st.session_state['df_curaleaf'] = df_curaleaf
    # Process Typesense data
    process_typesense_data()
