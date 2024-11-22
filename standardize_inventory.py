import pandas as pd
from sqlalchemy import create_engine, text
import toml

# Load database credentials from secrets.toml
secrets = toml.load('/Users/phoenix/Desktop/TGR/secrets.toml')
db_config = {
    "user": secrets['user'],
    "password": secrets['password'],
    "host": secrets['host'],
    "port": secrets['port'],
    "dbname": secrets['dbname']
}

# Connection string for SQLAlchemy
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

def create_table_if_not_exists():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS standardized_inventory (
        snapshot_date DATE,
        dispensary_name VARCHAR(255),
        product_name VARCHAR(255),
        price NUMERIC,
        quantity INTEGER,
        brand VARCHAR(255),
        category VARCHAR(255)
    );
    """
    with engine.connect() as connection:
        connection.execute(text(create_table_query))
        connection.commit()
        print("Table 'standardized_inventory' ensured to exist.")

def standardize_data():
    # Ensure the standardized_inventory table exists
    create_table_if_not_exists()

    # Query for iHeartJane table
    query_iheartjane = """
    SELECT 
        snapshot_time, 
        store_name AS dispensary_name, 
        product_name, 
        price, 
        quantity, 
        brand, 
        type AS category
    FROM "IheartJaneZen"
"""

# Query for Dutchie ZenRows table
    query_dutchiezen = """
        SELECT 
            snapshot_time, 
            dispensary_name, 
            product_name, 
            price, 
            quantity, 
            brand, 
            type AS category
        FROM "dutchieZen"
    """

    # Query for Typesense table
    query_typesense = """
        SELECT 
            snapshot_time, 
            "Location" AS dispensary_name, 
            "Product_Name" AS product_name, 
            "Price" AS price, 
            "Available_Quantity" AS quantity, 
            "Brand" AS brand, 
            "Category" AS category
        FROM typesense_table
    """

    # Fetch data from each table
    df_iheartjane = pd.read_sql_query(query_iheartjane, con=engine)
    df_dutchiezen = pd.read_sql_query(query_dutchiezen, con=engine)
    df_typesense = pd.read_sql_query(query_typesense, con=engine)

    # Combine all data into one DataFrame
    df_combined = pd.concat([df_iheartjane, df_dutchiezen, df_typesense], ignore_index=True)

    # Convert snapshot_time to snapshot_date (day-level data)
    df_combined['snapshot_date'] = pd.to_datetime(df_combined['snapshot_time']).dt.date

    # Group by snapshot_date, dispensary_name, product_name, price, brand, and category
    df_grouped = df_combined.groupby(
        ['snapshot_date', 'dispensary_name', 'product_name', 'price', 'brand', 'category'], 
        as_index=False
    ).agg({'quantity': 'sum'})  # Sum quantities to avoid duplicates

    # Fetch existing records from the standardized_inventory table
    existing_data = pd.read_sql_query('SELECT snapshot_date, dispensary_name, product_name FROM standardized_inventory', con=engine)

    # Identify new records
    new_data = df_grouped.merge(existing_data, on=['snapshot_date', 'dispensary_name', 'product_name'], how='left', indicator=True)
    new_data = new_data[new_data['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not new_data.empty:
        # Insert only new data into the standardized_inventory table
        new_data.to_sql('standardized_inventory', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"{len(new_data)} new records inserted.")
    else:
        print("No new data to insert.")

if __name__ == "__main__":
    standardize_data()
