-- FUNCTION: public.extract_category_name(text)

-- DROP FUNCTION IF EXISTS public.extract_category_name(text);

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
    -- Replace single quotes with double quotes
    well_formatted_json := REPLACE(category_text, '''', '"');
    
    -- Attempt to extract the name value
    BEGIN
        category_name := well_formatted_json::jsonb->>'name';
    EXCEPTION WHEN others THEN
        category_name := NULL;
    END;
    
    RETURN category_name;
END;
$BODY$;

ALTER FUNCTION public.extract_category_name(text)
    OWNER TO postgres;
