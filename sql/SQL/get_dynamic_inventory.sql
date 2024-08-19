-- FUNCTION: public.get_dynamic_inventory()

-- DROP FUNCTION IF EXISTS public.get_dynamic_inventory();

CREATE OR REPLACE FUNCTION public.get_dynamic_inventory(
	)
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
    -- Drop and recreate the table t_grower_circle_inventory_clean_normalized
    EXECUTE 'DROP TABLE IF EXISTS public.t_grower_circle_inventory_clean_normalized;';
    EXECUTE 'CREATE TABLE public.t_grower_circle_inventory_clean_normalized AS SELECT snapshot_timestamp, dispensary_name, name, price, quantity, category, subcategory, thc_content, brand, strain FROM public.v_grower_circle_inventory_clean_normalized;';

    -- Create the MaxTimeInventory table if it doesn't exist
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
    
    -- Insert only new MaxTimeInventory data
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

    -- Retrieve the distinct dates and construct the date columns for both original and reversed order
    FOR rec IN
        SELECT snapshot_date
        FROM public.t_max_time_inventory
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    LOOP
        date_columns_original := date_columns_original || 'MAX(CASE WHEN snapshot_date = ''' || rec.snapshot_date || ''' THEN quantity END) AS "' || rec.snapshot_date || '", ';
        date_columns_reversed := 'MAX(CASE WHEN snapshot_date = ''' || rec.snapshot_date || ''' THEN quantity END) AS "' || rec.snapshot_date || '", ' || date_columns_reversed;
    END LOOP;

    -- Remove trailing comma and space from the end of the string
    date_columns_original := rtrim(date_columns_original, ', ');
    date_columns_reversed := rtrim(date_columns_reversed, ', ');

    -- Drop the dynamic inventory table if it exists
    EXECUTE 'DROP TABLE IF EXISTS public.t_dynamic_inventory;';
    EXECUTE 'DROP TABLE IF EXISTS public.t_dynamic_inventory_reversedDates;';

    -- Construct the SQL query dynamically to create a new table with original aggregated data
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

    -- Construct the SQL query dynamically to create a new table with reversed aggregated data
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

ALTER FUNCTION public.get_dynamic_inventory()
    OWNER TO postgres;
