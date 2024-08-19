SELECT c.name as "type", b.strainname, quantity, b.thcpct, b.harvestcode, a.parentmetrcid, a.childmetrcid, packagedate
	FROM public.cultivationsubinventory AS a
		LEFT JOIN public.cultivationinventory AS b
			ON a.parentmetrcid = b.parentmetrcid
		LEFT JOIN public.producttypes as c
			ON a.producttypeid = cast(c.producttypeid as integer)
			
ORDER BY 1,2