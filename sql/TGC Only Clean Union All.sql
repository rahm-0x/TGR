SELECT snapshot_timestamp::timestamp without time zone
	, dispensary_name
	, name
	, price::real
	, quantity::integer
	, type
	, '' as subtype
	, thc_content::real
	, brand
FROM public.dispensary_inventory_snapshot
WHERE brand like '%Grower Circle%' or brand like '%Growers Circle%'

UNION ALL
	
-------------------------------------------------- 
	
SELECT snapshot_time::timestamp without time zone as snapshot_timestamp
	, "Location" as dispensary_name
	, "Product_Name" as name
	, bucket_price::real as price
	, max_cart_quantity::integer as quantity
	, kind as type
	, kind_subtype as subtype
	, "THC_Percent"::real as thc_content
	, "Brand" as brand
FROM public.iheartjane_table
WHERE "Brand" like '%Grower Circle%' or "Brand" like '%Growers Circle%'

-----------------------------------------------------------
union ALL

SELECT snapshot_time::timestamp without time zone as snapshot_timestamp
	, "Location" as dispensary_name
	, "Product_Name" as name
	, "Price"::real as price
	, "Available_Quantity"::integer as quantity
	, "Category" as type
	, "subcategoryName" as subtype
	, "thcPercentage"::real as thc_content
	, "Brand" as brand
	FROM public.typesense_table
	
WHERE "Brand" like '%Grower Circle%' or "Brand" like '%Growers Circle%'

-----------------------------------------------------------
union ALL

SELECT "Snapshot_Time"::timestamp without time zone as snapshot_timestamp
	 ,"Location" as dispensary_name
	 , "Product_Name" as name
	 , null as price
	 , null as quantity
	 , "Category" as type
	 , category as subtype
	, null as thc_content
	, null as brand
	FROM public.weedmaps_table
WHERE "Product_Name" like '%grower%' or "Product_Name" like '%Grower%'

z