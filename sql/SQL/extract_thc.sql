-- FUNCTION: public.extract_thc(text)

-- DROP FUNCTION IF EXISTS public.extract_thc(text);

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
    -- Replace single quotes with double quotes
    well_formatted_json := REPLACE(metrics_text, '''', '"');
    
    -- Attempt to extract the thc value
    BEGIN
        thc_value := (well_formatted_json::jsonb->'aggregates'->>'thc')::decimal;
    EXCEPTION WHEN others THEN
        thc_value := NULL;
    END;
    
    RETURN thc_value;
END;
$BODY$;

ALTER FUNCTION public.extract_thc(text)
    OWNER TO postgres;
