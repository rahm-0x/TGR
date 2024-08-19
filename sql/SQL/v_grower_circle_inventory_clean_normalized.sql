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