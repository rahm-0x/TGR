import psycopg2
import psycopg2.extras
import os
import json
import toml
import pandas as pd

def load_secrets(filepath):
    secrets = toml.load(filepath)
    db_config = {
        "user": secrets['user'],
        "password": secrets['password'],
        "host": secrets['host'],
        "port": secrets['port'],
        "dbname": secrets['dbname']
    }
    return db_config

def load_training_data(filepath):
    return pd.read_csv(filepath, header=None, names=[
        'Original_Name', 'Original_Category', 'Original_Subcategory', 
        'Full_Original_Name', 'Category', 'Subcategory', 'Strain'
    ])

def init_db_connection(db_config):
    return psycopg2.connect(
        host=db_config["host"],
        database=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"]
    )

def create_backup(cursor):
    backup_sql = """
    DO $$ 
    BEGIN 
        IF EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'sku_dispensary_tgc_mapping_backup'
        ) THEN 
            DROP TABLE public.sku_dispensary_tgc_mapping_backup; 
        END IF;
        CREATE TABLE public.sku_dispensary_tgc_mapping_backup AS TABLE public.sku_dispensary_tgc_mapping; 
    END $$;
    """
    cursor.execute(backup_sql)

def create_mapping_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sku_dispensary_tgc_mapping (
        dispensary_name VARCHAR,
        Product VARCHAR,
        Category VARCHAR,
        Subcategory VARCHAR,
        Translated_Category VARCHAR,
        Translated_Subcategory VARCHAR,
        Strain VARCHAR
    );
    """
    cursor.execute(create_table_query)

def fetch_data(cursor):
    query = """
    SELECT DISTINCT 
      IC.dispensary_name,
      IC.name, 
      IC.type,
      IC.subtype
    FROM 
      public.v_grower_circle_inventory_clean IC
      LEFT JOIN public.sku_dispensary_tgc_mapping MAP
      ON MAP.Product = IC.name and IC.dispensary_name = MAP.dispensary_name
    WHERE 
      IC.brand LIKE 'The Grower Circle'
      AND MAP.Product IS NULL
    """
    cursor.execute(query)
    return cursor.fetchall()

def map_data(rows, mapping_dict):
    result_json = []
    for i, row in enumerate(rows):
        dispensary_name = row[0]
        product_name = row[1]
        category_name = row[2]
        subcategory_name = row[3]
        
        if product_name in mapping_dict:
            translated_category, translated_subcategory, strain = mapping_dict[product_name]
            result_json.append({
                "Before": {
                    "dispensary_name": dispensary_name,
                    "name": product_name,
                    "type": category_name,
                    "subtype": subcategory_name
                },
                "After": f"{translated_category}, {translated_subcategory}, {strain}"
            })
        else:
            result_json.append({
                "Before": {
                    "dispensary_name": dispensary_name,
                    "name": product_name,
                    "type": category_name,
                    "subtype": subcategory_name
                },
                "After": ", , "
            })
    return result_json

def save_to_json(data, filepath):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

def insert_data(cursor, data):
    values = []
    for entry in data:
        after_split = entry["After"].strip().split(", ")
        if len(after_split) == 3:
            values.append((
                entry["Before"]["dispensary_name"],
                entry["Before"]["name"],
                entry["Before"]["type"],
                entry["Before"]["subtype"],
                after_split[0], after_split[1], after_split[2]
            ))

    insert_query = """
    INSERT INTO sku_dispensary_tgc_mapping
    (dispensary_name, product, category, subtype, translated_category, translated_subcategory, strain)
    VALUES %s;
    """
    psycopg2.extras.execute_values(cursor, insert_query, values)


def main():
    db_config = load_secrets("/Users/phoenix/Desktop/TGR/secrets.toml")
    training_df = load_training_data("/Users/phoenix/Desktop/TGR/training_data.csv")
    
    mapping_dict = {row['Original_Name']: (row['Category'], row['Subcategory'], row['Strain']) for _, row in training_df.iterrows()}

    conn = init_db_connection(db_config)
    cursor = conn.cursor()
    
    create_backup(cursor)
    conn.commit()
    
    create_mapping_table(cursor)
    conn.commit()
    
    rows = fetch_data(cursor)
    
    result_json = map_data(rows, mapping_dict)
    
    save_to_json(result_json, '/Users/phoenix/Desktop/TGC-sell_through/001-strain_names.json')
    
    insert_data(cursor, result_json)
    conn.commit()
    
    cursor.close()
    conn.close()
    print("Data processing complete.")

if __name__ == "__main__":
    main()
