-- FUNCTION: public.extract_price(text)

-- DROP FUNCTION IF EXISTS public.extract_price(text);

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
    -- Replace single quotes with double quotes
    well_formatted_json := REPLACE(price_text, '''', '"');
    -- Replace boolean values with lowercase
    well_formatted_json := REPLACE(REPLACE(well_formatted_json, 'True', 'true'), 'False', 'false');
    
    -- Attempt to extract the price value
    BEGIN
        price_value := (well_formatted_json::jsonb->>'price')::decimal;
    EXCEPTION WHEN others THEN
        price_value := NULL;
    END;
    
    RETURN price_value;
END;
$BODY$;

ALTER FUNCTION public.extract_price(text)
    OWNER TO postgres;
