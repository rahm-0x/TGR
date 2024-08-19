-- Existing Tables
CREATE TABLE IF NOT EXISTS public.curaleafData (
    snapshot_timestamp timestamp without time zone,
    dispensary_name text,
    name text,
    price real,
    quantity integer,
    type text,
    subtype text,
    thc_content text,
    brand text
);

CREATE TABLE IF NOT EXISTS public.cultivationinventory (
    parentmetrcid text,
    strainname text,
    thcpct real,
    harvestcode text,
    quantity integer
);

CREATE TABLE IF NOT EXISTS public.cultivationsubinventory (
    parentmetrcid text,
    childmetrcid text,
    producttypeid integer,
    packagedate date,
    quantity integer
);

CREATE TABLE IF NOT EXISTS public.customers (
    id serial PRIMARY KEY,
    name text
);

CREATE TABLE IF NOT EXISTS public.dispensary_inventory_snapshot (
    snapshot_timestamp timestamp without time zone,
    dispensary_id text,
    dispensary_name text,
    id text,
    name text,
    price real,
    quantity integer,
    type text,
    thc_content real,
    brand text,
    brand_id text
);

CREATE TABLE IF NOT EXISTS public.v_grower_circle_inventory_clean (
    dispensary_name VARCHAR,
    name VARCHAR,
    type VARCHAR,
    subtype VARCHAR,
    brand VARCHAR
);

CREATE TABLE IF NOT EXISTS public.iheartjane_table (
    snapshot_time timestamp without time zone,
    "Location" text,
    "Product_Name" text,
    bucket_price real,
    max_cart_quantity integer,
    kind text,
    kind_subtype text,
    "THC_Percent" real,
    "Brand" text
);

CREATE TABLE IF NOT EXISTS public.invoices (
    id serial PRIMARY KEY,
    invoicedate date,
    customerID integer REFERENCES public.customers(id)
);

CREATE TABLE IF NOT EXISTS public.items_invoiced (
    invoiceid integer REFERENCES public.invoices(id),
    productname text,
    unitprice real,
    description text,
    totalvalue real,
    quantity integer,
    accountname text
);

CREATE TABLE IF NOT EXISTS public.producttypes (
    producttypeid serial PRIMARY KEY,
    name text
);

CREATE TABLE IF NOT EXISTS public.typesense_table (
    "cacheTimestamp" numeric,
    "Location" text,
    "Product_Name" text,
    "Price" real,
    "Available_Quantity" integer,
    "Category" text,
    "subcategoryName" text,
    "thcPercentage" real,
    "Brand" text
);

CREATE TABLE IF NOT EXISTS public.weedmaps_table (
    "Snapshot_Time" timestamp without time zone,
    "Location" text,
    "Product_Name" text,
    "Price" text,
    "Category" text,
    "Metrics_Aggregates" text
);

-- New Tables
CREATE TABLE IF NOT EXISTS public.t_dynamic_inventory (
    dispensary_name text,
    brand text,
    name text,
    strain text,
    category text,
    subcategory text,
    thc_content text,
    price real,
    "2024-07-22" integer,
    "2024-07-28" integer
);

CREATE TABLE IF NOT EXISTS public.t_dynamic_inventory_reverseddates (
    dispensary_name text,
    brand text,
    name text,
    strain text,
    category text,
    subcategory text,
    thc_content text,
    price real,
    "2024-07-28" integer,
    "2024-07-22" integer
);

CREATE TABLE IF NOT EXISTS public.t_grower_circle_inventory_clean_normalized (
    snapshot_timestamp timestamp without time zone,
    dispensary_name text,
    name text,
    price real,
    quantity integer,
    category text,
    subcategory text,
    thc_content text,
    brand text,
    strain text
);

CREATE TABLE IF NOT EXISTS public.t_max_time_inventory (
    snapshot_date date,
    dispensary_name text,
    brand text,
    name text,
    strain text,
    category text,
    subcategory text,
    thc_content text,
    price real,
    quantity integer,
    max_time timestamp,
    UNIQUE (snapshot_date, dispensary_name, name, strain, category, subcategory, thc_content, price) -- Adding the UNIQUE constraint here
);

-- Table for Strain Name Mapping
CREATE TABLE IF NOT EXISTS public.sku_dispensary_tgc_mapping (
    dispensary_name VARCHAR,
    product VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    translated_category VARCHAR,
    translated_subcategory VARCHAR,
    strain VARCHAR
);

-- Add columns to sku_dispensary_tgc_mapping if they do not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sku_dispensary_tgc_mapping' AND column_name='translated_category') THEN
        ALTER TABLE public.sku_dispensary_tgc_mapping ADD COLUMN translated_category VARCHAR;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sku_dispensary_tgc_mapping' AND column_name='translated_subcategory') THEN
        ALTER TABLE public.sku_dispensary_tgc_mapping ADD COLUMN translated_subcategory VARCHAR;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sku_dispensary_tgc_mapping' AND column_name='strain') THEN
        ALTER TABLE public.sku_dispensary_tgc_mapping ADD COLUMN strain VARCHAR;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sku_dispensary_tgc_mapping' AND column_name='type') THEN
        ALTER TABLE public.sku_dispensary_tgc_mapping ADD COLUMN type VARCHAR;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sku_dispensary_tgc_mapping' AND column_name='subtype') THEN
        ALTER TABLE public.sku_dispensary_tgc_mapping ADD COLUMN subtype VARCHAR;
    END IF;
END $$;
