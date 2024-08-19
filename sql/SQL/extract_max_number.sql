-- FUNCTION: public.extract_max_number(text)

-- DROP FUNCTION IF EXISTS public.extract_max_number(text);

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
    -- Remove '%' and 'mg'
    cleaned_text := REPLACE(REPLACE($1, 'mg', ''), '%', '');

    -- Exit if cleaned_text is an empty string
    IF cleaned_text = '' THEN
        RETURN NULL;
    END IF;

    -- Extract numbers
    SELECT ARRAY(
        SELECT (regexp_matches(cleaned_text, '\d+(\.\d+)?', 'g'))[1]::real
    ) INTO all_numbers;

    -- Find the maximum number
    FOREACH num IN ARRAY all_numbers LOOP
        IF max_number IS NULL OR num > max_number THEN
            max_number := num;
        END IF;
    END LOOP;

    RETURN max_number;
END;
$BODY$;

ALTER FUNCTION public.extract_max_number(text)
    OWNER TO postgres;
