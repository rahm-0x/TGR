SELECT "Location" 
	, snapshot_time
	, "Product_Name"
	, "Description"
	, max_cart_quantity
	, aggregate_rating
	, review_count
	, amount
	, available_weights
	, "Brand"
	, bucket_price
	, kind_subtype
	, kind
	, custom_product_type
	, "CBD_Percent"
	, "THC_Percent"
	, product_id
	, root_subtype
	, type
	, sort_price
	, feelings
	, activities
	, "Category"
	, brand_subtype
	, max_cart_quantity_each
	
FROM public.iheartjane_table
	
WHERE "Brand" like '%Grower Circle%' or "Brand" like '%Growers Circle%'