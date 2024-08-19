
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

ALTER TABLE public.v_grower_circle_inventory_clean
    OWNER TO postgres;