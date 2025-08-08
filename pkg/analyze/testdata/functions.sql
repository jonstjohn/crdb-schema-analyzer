CREATE FUNCTION public."nextTicketNumber"(IN region string, IN locationid STRING)
      RETURNS INT8
      STABLE
      NOT LEAKPROOF
      CALLED ON NULL INPUT
      LANGUAGE SQL
      SECURITY INVOKER
      AS $$
      SELECT number + 1 FROM d.public.ticket_number WHERE (crdb_region = region) AND ("locationId" = locationid) ORDER BY number DESC LIMIT 1;
  $$;

 CREATE FUNCTION public."nextReceiptNumber"(IN region string, IN locationid STRING)
      RETURNS INT8
      STABLE
      NOT LEAKPROOF
      CALLED ON NULL INPUT
      LANGUAGE SQL
      SECURITY INVOKER
      AS $$
      SELECT number + 1 FROM d.receipt_number WHERE (crdb_region = region) AND ("locationId" = locationid) ORDER BY number DESC LIMIT 1;
  $$;
