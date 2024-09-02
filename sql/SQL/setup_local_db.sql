-- ===========================================
-- SECTION 1: CREATE TABLES
-- ===========================================
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
    UNIQUE (snapshot_date, dispensary_name, name, strain, category, subcategory, thc_content, price)
);

CREATE TABLE IF NOT EXISTS public.sku_dispensary_tgc_mapping (
    dispensary_name VARCHAR,
    product VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    translated_category VARCHAR,
    translated_subcategory VARCHAR,
    strain VARCHAR
);

-- ===========================================
-- SECTION 2: CREATE FUNCTIONS
-- ===========================================
CREATE OR REPLACE FUNCTION public.extract_category_name(
    category_text text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    well_formatted_json text;
    category_name text;
BEGIN
    well_formatted_json := REPLACE(category_text, '''', '"');
    BEGIN
        category_name := well_formatted_json::jsonb->>'name';
    EXCEPTION WHEN others THEN
        category_name := NULL;
    END;
    RETURN category_name;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.extract_price(
    price_text text)
    RETURNS numeric
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    well_formatted_json text;
    price_value decimal;
BEGIN
    well_formatted_json := REPLACE(price_text, '''', '"');
    well_formatted_json := REPLACE(REPLACE(well_formatted_json, 'True', 'true'), 'False', 'false');
    BEGIN
        price_value := (well_formatted_json::jsonb->>'price')::decimal;
    EXCEPTION WHEN others THEN
        price_value := NULL;
    END;
    RETURN price_value;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.extract_thc(
    metrics_text text)
    RETURNS numeric
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    well_formatted_json text;
    thc_value decimal;
BEGIN
    well_formatted_json := REPLACE(metrics_text, '''', '"');
    BEGIN
        thc_value := (well_formatted_json::jsonb->'aggregates'->>'thc')::decimal;
    EXCEPTION WHEN others THEN
        thc_value := NULL;
    END;
    RETURN thc_value;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.extract_max_number(
    text)
    RETURNS real
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    cleaned_text text;
    all_numbers text[];
    max_number real := NULL;
    num real;
BEGIN
    cleaned_text := REPLACE(REPLACE($1, 'mg', ''), '%', '');
    IF cleaned_text = '' THEN
        RETURN NULL;
    END IF;
    SELECT ARRAY(
        SELECT (regexp_matches(cleaned_text, '\d+(\.\d+)?', 'g'))[1]::real
    ) INTO all_numbers;
    FOREACH num IN ARRAY all_numbers LOOP
        IF max_number IS NULL OR num > max_number THEN
            max_number := num;
        END IF;
    END LOOP;
    RETURN max_number;
END;
$BODY$;

-- ===========================================
-- SECTION 3: CREATE VIEWS
-- ===========================================
CREATE OR REPLACE VIEW public.v_grower_circle_inventory_clean AS
SELECT 
    dispensary_inventory_snapshot.snapshot_timestamp::timestamp without time zone AS snapshot_timestamp,
    dispensary_inventory_snapshot.dispensary_name,
    dispensary_inventory_snapshot.name,
    dispensary_inventory_snapshot.price,
    dispensary_inventory_snapshot.quantity,
    dispensary_inventory_snapshot.type,
    ''::text AS subtype,
    dispensary_inventory_snapshot.thc_content,
    CASE
        WHEN dispensary_inventory_snapshot.brand ~~ '%flight%'::text 
        OR dispensary_inventory_snapshot.brand ~~ '%Flight%'::text 
        OR dispensary_inventory_snapshot.brand ~~ '%Growers Circle%'::text 
        OR dispensary_inventory_snapshot.brand ~~ '%Grower Circle%'::text 
        OR dispensary_inventory_snapshot.brand ~~ '%Vegas Valley Growers%'::text 
        THEN 'The Grower Circle'::text
        ELSE dispensary_inventory_snapshot.brand
    END AS brand
FROM dispensary_inventory_snapshot
UNION ALL
SELECT 
    iheartjane_table.snapshot_time::timestamp without time zone AS snapshot_timestamp,
    iheartjane_table."Location" AS dispensary_name,
    iheartjane_table."Product_Name" AS name,
    iheartjane_table.bucket_price::real AS price,
    iheartjane_table.max_cart_quantity::integer AS quantity,
    iheartjane_table.kind AS type,
    iheartjane_table.kind_subtype AS subtype,
    iheartjane_table."THC_Percent"::real AS thc_content,
    CASE
        WHEN iheartjane_table."Brand" ~~ '%flight%'::text 
        OR iheartjane_table."Brand" ~~ '%Flight%'::text 
        OR iheartjane_table."Brand" ~~ '%Grower Circle%'::text 
        OR iheartjane_table."Brand" ~~ '%Growers Circle%'::text 
        OR iheartjane_table."Brand" ~~ '%Vegas Valley Growers%'::text 
        THEN 'The Grower Circle'::text
        ELSE iheartjane_table."Brand"
    END AS brand
FROM iheartjane_table
UNION ALL
SELECT 
    TO_TIMESTAMP((typesense_table."cacheTimestamp"::numeric / 1000)::double precision) AS snapshot_timestamp,
    typesense_table."Location" AS dispensary_name,
    typesense_table."Product_Name" AS name,
    typesense_table."Price"::real AS price,
    typesense_table."Available_Quantity"::integer AS quantity,
    typesense_table."Category" AS type,
    typesense_table."subcategoryName" AS subtype,
    typesense_table."thcPercentage"::real AS thc_content,
    CASE
        WHEN typesense_table."Brand" ~~ '%flight%'::text 
        OR typesense_table."Brand" ~~ '%Flight%'::text 
        OR typesense_table."Brand" ~~ '%Grower Circle%'::text 
        OR typesense_table."Brand" ~~ '%Growers Circle%'::text 
        OR typesense_table."Brand" ~~ '%GROWERS CIRCLE%'::text 
        OR typesense_table."Brand" ~~ '%Vegas Valley Growers%'::text 
        THEN 'The Grower Circle'::text
        ELSE typesense_table."Brand"
    END AS brand
FROM typesense_table
UNION ALL
SELECT 
    weedmaps_table."Snapshot_Time"::timestamp without time zone AS snapshot_timestamp,
    weedmaps_table."Location" AS dispensary_name,
    weedmaps_table."Product_Name" AS name,
    COALESCE(to_char(extract_price(weedmaps_table."Price"), 'FM999999999990.00'::text), '0.00'::text)::real AS price,
    1 AS quantity,
    COALESCE(extract_category_name(weedmaps_table."Category"), 'Unknown'::text) AS type,
    COALESCE(extract_category_name(weedmaps_table."Category"), 'Unknown'::text) AS subtype,
    COALESCE(extract_thc(weedmaps_table."Metrics_Aggregates"), 0.0) AS thc_content,
    CASE
        WHEN weedmaps_table."Product_Name" ~~ '%flight%'::text 
        OR weedmaps_table."Product_Name" ~~ '%Flight%'::text 
        OR weedmaps_table."Product_Name" ~~ '%Grower%'::text 
        OR weedmaps_table."Product_Name" ~~ '%VVG%'::text 
        THEN 'The Grower Circle'::text
        ELSE 'Not TGC'::text
    END AS brand
FROM weedmaps_table
UNION ALL
SELECT 
    curaleafdata.snapshot_timestamp,
    curaleafdata.dispensary_name::text AS dispensary_name,
    curaleafdata.name::text AS name,
    curaleafdata.price::real AS price,
    curaleafdata.quantity,
    curaleafdata.type::text AS type,
    curaleafdata.subtype::text AS subtype,
    extract_max_number(curaleafdata.thc_content::text) AS thc_content,
    CASE
        WHEN curaleafdata.brand::text ~~ '%flight%'::text 
        OR curaleafdata.brand::text ~~ '%Flight%'::text 
        OR curaleafdata.brand::text ~~ '%Grower Circle%'::text 
        OR curaleafdata.brand::text ~~ '%Growers Circle%'::text 
        OR curaleafdata.brand::text ~~ '%Vegas Valley Growers%'::text 
        THEN 'The Grower Circle'::text
        ELSE curaleafdata.brand::text
    END AS brand
FROM curaleafdata;

CREATE OR REPLACE VIEW public.v_grower_circle_inventory_clean_normalized AS
SELECT 
    ic.snapshot_timestamp,
    ic.dispensary_name,
    ic.name,
    ic.price,
    ic.quantity,
    COALESCE(map.translated_category, ic.type) AS category,
    COALESCE(map.translated_subcategory, ic.subtype) AS subcategory,
    ic.thc_content,
    ic.brand,
    map.strain
FROM 
    v_grower_circle_inventory_clean ic
LEFT JOIN 
    sku_dispensary_tgc_mapping map 
ON 
    TRIM(BOTH FROM lower(map.dispensary_name)) = TRIM(BOTH FROM lower(ic.dispensary_name)) 
    AND TRIM(BOTH FROM lower(map.product)) = TRIM(BOTH FROM lower(ic.name));

-- Create table t_grower_circle_inventory_clean_normalized if it does not exist
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 't_grower_circle_inventory_clean_normalized') THEN
        CREATE TABLE public.t_grower_circle_inventory_clean_normalized AS
        SELECT * FROM public.v_grower_circle_inventory_clean_normalized LIMIT 0;
    END IF;
END $$;

ALTER TABLE public.t_grower_circle_inventory_clean_normalized
    OWNER TO postgres;

-- ===========================================
-- SECTION 4: DYNAMIC INVENTORY FUNCTIONS
-- ===========================================
CREATE OR REPLACE FUNCTION public.get_dynamic_inventory()
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL SAFE 
AS $BODY$
DECLARE
    rec record;
    date_column text;
    date_columns_original text := '';
    date_columns_reversed text := '';
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS public.t_grower_circle_inventory_clean_normalized;';
    EXECUTE 'CREATE TABLE public.t_grower_circle_inventory_clean_normalized AS SELECT snapshot_timestamp, dispensary_name, name, price, quantity, category, subcategory, thc_content, brand, strain FROM public.v_grower_circle_inventory_clean_normalized;';

    EXECUTE 'CREATE TABLE IF NOT EXISTS public.t_max_time_inventory (
        snapshot_date DATE,
        dispensary_name TEXT,
        brand TEXT,
        name TEXT,
        strain TEXT,
        category TEXT,
        subcategory TEXT,
        thc_content TEXT,
        price NUMERIC,
        quantity INTEGER,
        max_time TIMESTAMP
    );';
    
    EXECUTE '
    INSERT INTO public.t_max_time_inventory (
        SELECT
            DATE(snapshot_timestamp) AS snapshot_date,
            dispensary_name,
            brand,
            name,
            strain,
            category,
            subcategory,
            thc_content,
            price,
            quantity,
            MAX(snapshot_timestamp) OVER (
                PARTITION BY dispensary_name, name, strain, category, subcategory, thc_content, price, DATE(snapshot_timestamp)
            ) AS max_time
        FROM
            public.t_grower_circle_inventory_clean_normalized
        WHERE
            snapshot_timestamp > COALESCE((SELECT MAX(max_time) FROM public.t_max_time_inventory), ''1970-01-01'')
    ) ON CONFLICT (snapshot_date, dispensary_name, name, strain, category, subcategory, thc_content, price)
    DO NOTHING;
    ';

    FOR rec IN
        SELECT snapshot_date
        FROM public.t_max_time_inventory
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    LOOP
        date_columns_original := date_columns_original || 'MAX(CASE WHEN snapshot_date = ''' || rec.snapshot_date || ''' THEN quantity END) AS "' || rec.snapshot_date || '", ';
        date_columns_reversed := 'MAX(CASE WHEN snapshot_date = ''' || rec.snapshot_date || ''' THEN quantity END) AS "' || rec.snapshot_date || '", ' || date_columns_reversed;
    END LOOP;

    date_columns_original := rtrim(date_columns_original, ', ');
    date_columns_reversed := rtrim(date_columns_reversed, ', ');

    EXECUTE 'DROP TABLE IF EXISTS public.t_dynamic_inventory;';
    EXECUTE 'DROP TABLE IF EXISTS public.t_dynamic_inventory_reversedDates;';

    EXECUTE '
    CREATE TABLE public.t_dynamic_inventory AS (
        SELECT
            dispensary_name,
            brand,
            name,
            strain,
            category,
            subcategory,
            thc_content,
            price, ' || date_columns_original || '
        FROM public.t_max_time_inventory
        GROUP BY dispensary_name, brand, name, strain, category, subcategory, thc_content, price
        ORDER BY dispensary_name, category, subcategory, brand, strain
    );';

    EXECUTE '
    CREATE TABLE public.t_dynamic_inventory_reversedDates AS (
        SELECT
            dispensary_name,
            brand,
            name,
            strain,
            category,
            subcategory,
            thc_content,
            price, ' || date_columns_reversed || '
        FROM public.t_max_time_inventory
        GROUP BY dispensary_name, brand, name, strain, category, subcategory, thc_content, price
        ORDER BY dispensary_name, category, subcategory, brand, strain
    );';
END;
$BODY$;
