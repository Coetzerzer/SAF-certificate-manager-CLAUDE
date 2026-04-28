-- Yearly PoC allocation with recipient/customer filter.
--
-- Adds a new "supported_yearly" / "simple_yearly_airport" allocation path for
-- annual single-airport PoCs (Hellenic Fuels / EKO format). Unlike the simple
-- monthly allocator, this one:
--   1. Spans the full calendar year (coverageStart=YYYY-01-01 → coverageEnd=YYYY-12-31)
--   2. Filters invoice rows by normalized customer name = normalized cert recipient
--      (prevents cross-allocation between operators sharing an airport)
--
-- The simple/monthly RPC (allocate_simple_certificate) is left untouched.

------------------------------------------------------------------------------
-- 1. Helper: normalize_corp_name(text) → text
--    Lowercase, strip punctuation/whitespace, drop common corporate suffixes
--    so that "AEROWEST GMBH" matches "Aerowest GmbH".
------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.normalize_corp_name(p_name text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $func$
DECLARE
  v_normalized text;
  v_suffix text;
  v_suffixes text[] := ARRAY[
    'gmbh', 'sa', 'sarl', 'srl', 'sro', 'spa', 'ag', 'kg',
    'ltd', 'llc', 'inc', 'corp', 'co',
    'bv', 'nv', 'as', 'ab', 'oy', 'oyj', 'aps', 'plc',
    'sp', 'pte', 'pty',
    'singlemember', 'industrial', 'commercial'
  ];
BEGIN
  IF p_name IS NULL THEN RETURN ''; END IF;
  -- Lowercase + strip non-alphanumeric (drops spaces, punctuation, dots, accents)
  v_normalized := lower(regexp_replace(p_name, '[^a-zA-Z0-9]+', '', 'g'));
  IF v_normalized = '' THEN RETURN ''; END IF;
  -- Strip trailing corporate suffixes iteratively (e.g. "...singlemember...sa" → "...")
  LOOP
    DECLARE
      v_changed boolean := false;
    BEGIN
      FOREACH v_suffix IN ARRAY v_suffixes LOOP
        IF v_normalized LIKE '%' || v_suffix AND length(v_normalized) > length(v_suffix) THEN
          v_normalized := substring(v_normalized FROM 1 FOR length(v_normalized) - length(v_suffix));
          v_changed := true;
        END IF;
      END LOOP;
      EXIT WHEN NOT v_changed;
    END;
  END LOOP;
  RETURN v_normalized;
END
$func$;

GRANT EXECUTE ON FUNCTION public.normalize_corp_name(text) TO anon, authenticated, service_role;

------------------------------------------------------------------------------
-- 2. RPC: allocate_yearly_certificate
--    FIFO-allocate a yearly PoC's SAF volume against invoice rows that match
--    on (airport, year, recipient). Calque structurel d'allocate_simple_certificate.
------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.allocate_yearly_certificate(
  p_certificate_id uuid,
  p_import_id uuid DEFAULT NULL,
  p_actor text DEFAULT NULL,
  p_force_reallocate boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
declare
  v_certificate record;
  v_certificate_volume numeric(18,6);
  v_coverage_start text;
  v_coverage_end text;
  v_iata_codes text[];
  v_icao_codes text[];
  v_period_start date;
  v_period_end date;
  v_recipient text;
  v_recipient_norm text;
  v_remaining_volume numeric(18,6);
  v_allocated_volume numeric(18,6) := 0;
  v_available_volume numeric(18,6) := 0;
  v_row record;
  v_take numeric(18,6);
  v_linked_rows jsonb := '[]'::jsonb;
  v_status text;
  v_match_method text;
  v_diagnostics jsonb;
  v_candidate_count int := 0;
  v_total_row_count int := 0;
  v_match_id uuid;
  v_import_status text;
  v_tolerance numeric(18,6) := 0.002;
  v_old_linked_row_ids uuid[];
  v_existing_approved record;
  v_available_normalized_customers jsonb;
begin
  select *
  into v_certificate
  from public.certificates
  where id = p_certificate_id
  for update;

  if not found then
    raise exception 'Certificate % was not found', p_certificate_id;
  end if;

  -- PROTECTION: Skip if already approved (unless forced)
  if not p_force_reallocate then
    select id, match_method, cert_volume_m3, allocated_volume_m3, variance_m3, diagnostics
    into v_existing_approved
    from public.certificate_matches
    where certificate_id = p_certificate_id and status = 'approved'
    limit 1;

    if found then
      return jsonb_build_object(
        'status', 'approved',
        'skipped', true,
        'match_method', v_existing_approved.match_method,
        'cert_volume_m3', v_existing_approved.cert_volume_m3,
        'allocated_volume_m3', v_existing_approved.allocated_volume_m3,
        'variance_m3', v_existing_approved.variance_m3,
        'diagnostics', v_existing_approved.diagnostics
      );
    end if;
  end if;

  -- Clear any prior match (and unflag previously allocated rows that are no longer linked)
  select array_agg(distinct cil.invoice_row_id)
  into v_old_linked_row_ids
  from public.certificate_invoice_links cil
  join public.certificate_matches cm on cm.id = cil.certificate_match_id
  where cm.certificate_id = p_certificate_id;

  delete from public.certificate_matches
  where certificate_id = p_certificate_id;

  if v_old_linked_row_ids is not null then
    update public.invoice_rows ir
    set is_allocated = false
    where ir.id = any(v_old_linked_row_ids)
      and ir.is_allocated = true
      and not exists (
        select 1 from public.certificate_invoice_links cil
        where cil.invoice_row_id = ir.id
      );
  end if;

  -- Classification gate: this RPC only handles supported_yearly / simple_yearly_airport
  if coalesce(v_certificate.document_family, '') <> 'supported_yearly'
  or coalesce(v_certificate.matching_mode, '') <> 'simple_yearly_airport' then
    v_status := 'manual_only';
    v_match_method := 'classification-reject';
    v_diagnostics := jsonb_build_object(
      'reason', 'Certificate is not classified as supported_yearly / simple_yearly_airport',
      'document_family', coalesce(v_certificate.document_family, ''),
      'matching_mode', coalesce(v_certificate.matching_mode, '')
    );
    insert into public.certificate_matches (
      id, certificate_id, status, match_method,
      cert_volume_m3, allocated_volume_m3, variance_m3,
      diagnostics, created_at, updated_at
    ) values (
      gen_random_uuid(), p_certificate_id, v_status, v_match_method,
      0, 0, 0,
      v_diagnostics, now(), now()
    );
    return jsonb_build_object('status', v_status, 'match_method', v_match_method, 'diagnostics', v_diagnostics);
  end if;

  -- Read certificate fields
  v_coverage_start := nullif(coalesce(v_certificate.data->>'coverageStart', ''), '');
  v_coverage_end := nullif(coalesce(v_certificate.data->>'coverageEnd', ''), '');
  v_recipient := coalesce(v_certificate.data->>'recipient', '');
  v_recipient_norm := public.normalize_corp_name(v_recipient);
  v_certificate_volume := coalesce(
    public.parse_flexible_numeric(v_certificate.data->>'quantity'), 0
  );

  select array_agg(distinct code) filter (where code <> '')
  into v_iata_codes
  from (
    select upper(coalesce(elem->>'iata', '')) as code
    from jsonb_array_elements(coalesce(v_certificate.data->'canonicalAirports', '[]'::jsonb)) elem
  ) sub;

  select array_agg(distinct code) filter (where code <> '')
  into v_icao_codes
  from (
    select upper(coalesce(elem->>'icao', '')) as code
    from jsonb_array_elements(coalesce(v_certificate.data->'canonicalAirports', '[]'::jsonb)) elem
  ) sub;

  if v_certificate.data->'physicalDeliveryAirportCanonical' is not null then
    declare
      v_pda_iata text := upper(coalesce(v_certificate.data->'physicalDeliveryAirportCanonical'->>'iata', ''));
      v_pda_icao text := upper(coalesce(v_certificate.data->'physicalDeliveryAirportCanonical'->>'icao', ''));
    begin
      if v_pda_iata <> '' and (v_iata_codes is null or not v_pda_iata = any(v_iata_codes)) then
        v_iata_codes := array_append(coalesce(v_iata_codes, array[]::text[]), v_pda_iata);
      end if;
      if v_pda_icao <> '' and (v_icao_codes is null or not v_pda_icao = any(v_icao_codes)) then
        v_icao_codes := array_append(coalesce(v_icao_codes, array[]::text[]), v_pda_icao);
      end if;
    end;
  end if;

  -- Validate inputs
  if v_coverage_start is null or v_coverage_end is null
  or v_coverage_start !~ '^\d{4}-\d{2}-\d{2}$' or v_coverage_end !~ '^\d{4}-\d{2}-\d{2}$'
  or v_certificate_volume <= 0
  or v_recipient_norm = ''
  or (coalesce(cardinality(v_iata_codes), 0) = 0 and coalesce(cardinality(v_icao_codes), 0) = 0) then
    v_status := 'manual_only';
    v_match_method := 'missing-key-data';
    v_diagnostics := jsonb_build_object(
      'reason', 'Missing coverage period, volume, recipient, or airport codes',
      'coverage_start', coalesce(v_coverage_start, ''),
      'coverage_end', coalesce(v_coverage_end, ''),
      'certificate_volume', v_certificate_volume,
      'recipient', v_recipient,
      'normalized_recipient', v_recipient_norm,
      'iata_codes', to_jsonb(coalesce(v_iata_codes, array[]::text[])),
      'icao_codes', to_jsonb(coalesce(v_icao_codes, array[]::text[]))
    );
    insert into public.certificate_matches (
      id, certificate_id, status, match_method,
      cert_volume_m3, allocated_volume_m3, variance_m3,
      diagnostics, created_at, updated_at
    ) values (
      gen_random_uuid(), p_certificate_id, v_status, v_match_method,
      v_certificate_volume, 0, v_certificate_volume,
      v_diagnostics, now(), now()
    );
    return jsonb_build_object('status', v_status, 'match_method', v_match_method, 'diagnostics', v_diagnostics);
  end if;

  if p_import_id is null then
    raise exception 'An active invoice import is required for supported_yearly allocation';
  end if;

  select status into v_import_status
  from public.invoice_imports
  where id = p_import_id;
  if not found then
    raise exception 'Invoice import % was not found', p_import_id;
  end if;
  if v_import_status <> 'active' then
    raise exception 'Invoice import % has status "%" — only active imports can be used for allocation', p_import_id, v_import_status;
  end if;

  v_period_start := v_coverage_start::date;
  v_period_end := v_coverage_end::date;
  if v_period_end < v_period_start then
    v_status := 'manual_only';
    v_match_method := 'invalid-coverage-period';
    v_diagnostics := jsonb_build_object(
      'reason', format('Coverage end %s is before start %s', v_period_end, v_period_start)
    );
    insert into public.certificate_matches (
      id, certificate_id, status, match_method, cert_volume_m3, allocated_volume_m3, variance_m3, diagnostics, created_at, updated_at
    ) values (gen_random_uuid(), p_certificate_id, v_status, v_match_method, v_certificate_volume, 0, v_certificate_volume, v_diagnostics, now(), now());
    return jsonb_build_object('status', v_status, 'match_method', v_match_method, 'diagnostics', v_diagnostics);
  end if;

  v_remaining_volume := v_certificate_volume;

  -- Total candidate row count (for diagnostics — pre-recipient-filter)
  select count(*)
  into v_total_row_count
  from public.invoice_rows ir
  where ir.import_id = p_import_id
    and ir.uplift_date >= v_period_start
    and ir.uplift_date <= v_period_end
    and (
      (coalesce(cardinality(v_iata_codes), 0) > 0 and upper(coalesce(ir.iata, '')) = any(v_iata_codes))
      or (coalesce(cardinality(v_icao_codes), 0) > 0 and upper(coalesce(ir.icao, '')) = any(v_icao_codes))
    );

  -- Available customers at this airport in this period (for debug if recipient mismatch)
  select coalesce(jsonb_agg(distinct sub.customer_norm) filter (where sub.customer_norm <> ''), '[]'::jsonb)
  into v_available_normalized_customers
  from (
    select public.normalize_corp_name(coalesce(ir.customer, '')) as customer_norm
    from public.invoice_rows ir
    where ir.import_id = p_import_id
      and ir.uplift_date >= v_period_start
      and ir.uplift_date <= v_period_end
      and (
        (coalesce(cardinality(v_iata_codes), 0) > 0 and upper(coalesce(ir.iata, '')) = any(v_iata_codes))
        or (coalesce(cardinality(v_icao_codes), 0) > 0 and upper(coalesce(ir.icao, '')) = any(v_icao_codes))
      )
  ) sub;

  -- FIFO loop: filter by airport + period + recipient match
  for v_row in
    with locked_candidates as (
      select
        ir.id, ir.row_number, ir.invoice_no, ir.customer, ir.uplift_date, ir.iata, ir.icao,
        coalesce(ir.saf_vol_m3, 0)::numeric(18,6) as saf_vol_m3
      from public.invoice_rows ir
      where ir.import_id = p_import_id
        and ir.uplift_date >= v_period_start
        and ir.uplift_date <= v_period_end
        and (
          (coalesce(cardinality(v_iata_codes), 0) > 0 and upper(coalesce(ir.iata, '')) = any(v_iata_codes))
          or (coalesce(cardinality(v_icao_codes), 0) > 0 and upper(coalesce(ir.icao, '')) = any(v_icao_codes))
        )
        and public.normalize_corp_name(coalesce(ir.customer, '')) = v_recipient_norm
      order by ir.uplift_date asc, coalesce(ir.invoice_no, '') asc, coalesce(ir.row_number, 0) asc, ir.id asc
      for update
    )
    select lc.*,
      lc.saf_vol_m3 - coalesce(
        (select sum(cil.allocated_m3) from public.certificate_invoice_links cil where cil.invoice_row_id = lc.id), 0
      ) as remaining_saf_m3
    from locked_candidates lc
  loop
    if v_row.remaining_saf_m3 <= 0.000001 then continue; end if;
    v_candidate_count := v_candidate_count + 1;
    v_available_volume := round(v_available_volume + v_row.remaining_saf_m3, 6);

    v_take := least(v_remaining_volume, v_row.remaining_saf_m3);
    v_take := round(v_take, 6);
    if v_take <= 0 then continue; end if;

    v_linked_rows := v_linked_rows || jsonb_build_object(
      'invoice_row_id', v_row.id, 'row_number', v_row.row_number,
      'invoice_no', v_row.invoice_no, 'customer', v_row.customer,
      'uplift_date', v_row.uplift_date, 'iata', v_row.iata, 'icao', v_row.icao,
      'allocated_m3', v_take
    );

    v_allocated_volume := round(v_allocated_volume + v_take, 6);
    v_remaining_volume := round(greatest(0, v_certificate_volume - v_allocated_volume), 6);
    if v_remaining_volume <= v_tolerance then exit; end if;
  end loop;

  v_match_method := 'fifo-yearly-recipient-scoped';

  if v_allocated_volume + v_tolerance >= v_certificate_volume then
    v_status := 'auto_linked';
  elsif v_allocated_volume > 0 then
    v_status := 'partial_linked';
  else
    v_status := 'unmatched';
  end if;

  v_diagnostics := jsonb_build_object(
    'coverage_granularity', 'year',
    'period_start', v_period_start,
    'period_end', v_period_end,
    'iata_codes', to_jsonb(coalesce(v_iata_codes, array[]::text[])),
    'icao_codes', to_jsonb(coalesce(v_icao_codes, array[]::text[])),
    'recipient', v_recipient,
    'normalized_recipient', v_recipient_norm,
    'available_normalized_customers', v_available_normalized_customers,
    'certificate_volume', v_certificate_volume,
    'allocated_volume', v_allocated_volume,
    'available_volume', v_available_volume,
    'candidate_count', v_candidate_count,
    'total_row_count_pre_recipient_filter', v_total_row_count,
    'linked_count', jsonb_array_length(v_linked_rows),
    'variance', round(v_certificate_volume - v_allocated_volume, 6),
    'actor', coalesce(p_actor, 'system'),
    'force_reallocate', p_force_reallocate
  );

  v_match_id := gen_random_uuid();

  insert into public.certificate_matches (
    id, certificate_id, status, match_method,
    cert_volume_m3, allocated_volume_m3, variance_m3,
    diagnostics, created_at, updated_at
  ) values (
    v_match_id, p_certificate_id, v_status, v_match_method,
    v_certificate_volume, v_allocated_volume,
    round(v_certificate_volume - v_allocated_volume, 6),
    v_diagnostics, now(), now()
  );

  if v_status in ('auto_linked', 'partial_linked') then
    insert into public.certificate_invoice_links (
      id, certificate_match_id, certificate_id, invoice_row_id,
      row_number, invoice_no, customer, uplift_date, iata, icao,
      allocated_m3, created_at
    )
    select
      gen_random_uuid(), v_match_id, p_certificate_id,
      (item->>'invoice_row_id')::uuid, (item->>'row_number')::int,
      item->>'invoice_no', item->>'customer', (item->>'uplift_date')::date,
      item->>'iata', item->>'icao', (item->>'allocated_m3')::numeric(18,6), now()
    from jsonb_array_elements(v_linked_rows) item;

    update public.invoice_rows ir
    set is_allocated = true
    where ir.id in (
      select (item->>'invoice_row_id')::uuid
      from jsonb_array_elements(v_linked_rows) item
    );
  end if;

  return jsonb_build_object(
    'status', v_status,
    'match_method', v_match_method,
    'cert_volume_m3', v_certificate_volume,
    'allocated_volume_m3', v_allocated_volume,
    'variance_m3', round(v_certificate_volume - v_allocated_volume, 6),
    'linked_count', jsonb_array_length(v_linked_rows),
    'candidate_count', v_candidate_count,
    'total_row_count', v_total_row_count,
    'diagnostics', v_diagnostics
  );
end;
$function$;

GRANT EXECUTE ON FUNCTION public.allocate_yearly_certificate(uuid, uuid, text, boolean) TO anon, authenticated, service_role;

------------------------------------------------------------------------------
-- 3. Re-classify existing manual_only certs that match the yearly pattern
--    (Hellenic / EKO-style PoCs already in DB before this migration).
--    Also drop any prior manual_only matches so analyzeAll can re-evaluate them.
------------------------------------------------------------------------------
WITH targets AS (
  SELECT id
  FROM public.certificates
  WHERE document_family = 'manual_only'
    -- Match either "Proof of Compliance" or just "PoC" (some extractions abbreviate)
    AND data->>'docType' ILIKE '%poc%'
    AND data->>'coverageStart' ~ '^\d{4}-01-01$'
    AND data->>'coverageEnd' ~ '^\d{4}-12-31$'
    AND substring(data->>'coverageStart' FROM 1 FOR 4) = substring(data->>'coverageEnd' FROM 1 FOR 4)
    AND lower(coalesce(data->>'quantityUnit', '')) = 'm3'
    AND coalesce(data->>'recipient', '') <> ''
    AND jsonb_array_length(coalesce(data->'canonicalAirports', '[]'::jsonb)) = 1
    AND coalesce(jsonb_array_length(data->'monthlyVolumes'), 0) = 0
    AND coalesce(jsonb_array_length(data->'airportVolumes'), 0) = 0
)
UPDATE public.certificates c
SET document_family = 'supported_yearly',
    matching_mode = 'simple_yearly_airport',
    classification_reason = 'Re-classified by 20260428 migration: yearly single-airport PoC with recipient',
    review_required = false
FROM targets t
WHERE c.id = t.id;

-- Drop their existing manual_only matches so analyzeAll re-runs allocation.
DELETE FROM public.certificate_matches cm
USING public.certificates c
WHERE cm.certificate_id = c.id
  AND c.document_family = 'supported_yearly'
  AND c.matching_mode = 'simple_yearly_airport'
  AND cm.status = 'manual_only';
