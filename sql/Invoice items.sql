SELECT productname, unitprice, description, a.totalvalue, quantity, accountname, b.invoicedate, c.name as "Customer"
	FROM public.items_invoiced AS a
		INNER JOIN public.invoices AS b
			ON a.invoiceid = b.id
		LEFT JOIN public.customers AS c
			ON b.customerID = c.id