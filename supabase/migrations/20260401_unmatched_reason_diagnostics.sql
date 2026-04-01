-- Migration: Add total_row_count to allocation diagnostics
-- Distinguishes "no invoice rows exist for this airport+month" from "rows exist but volume is fully consumed"

create or replace function public.allocate_simple_certificate(
  p_certificate_id uuid,
  p_import_id uuid default null,
  p_actor text default null
)
returns jsonb
language plpgsql
security definer
as $$
declare
  v_certificate record;
  v_certificate_volume numeric(18,6);
  v_coverage_month text;
  v_iata_codes text[];
  v_icao_codes text[];
  v_month_start date;
  v_month_end date;
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
begin
  -- Lock certificate row
  select *
  into v_certificate
  from public.certificates
  where id = p_certificate_id
  for update;

  if not found then
    raise exception 'Certificate % was not found', p_certificate_id;
  end if;

  -- Delete prior match + links for this certificate
  delete from public.certificate_matches
  where certificate_id = p_certificate_id;

  -- Check classification
  if coalesce(v_certificate.document_family, '') <> 'supported_simple'
  or coalesce(v_certificate.matching_mode, '') <> 'simple_monthly_airport' then
    v_status := 'manual_only';
    v_match_method := 'classification-reject';
    v_diagnostics := jsonb_build_object(
      'reason', 'Certificate is not classified as supported_simple / simple_monthly_airport',
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

  -- Extract key fields
  v_coverage_month := coalesce(v_certificate.data->>'coverageMonth', '');
  v_certificate_volume := coalesce(
    public.parse_flexible_numeric(v_certificate.data->>'quantity'), 0
  );

  -- Extract airport codes from canonicalAirports JSON array
  select array_agg(distinct code) filter (where code <> '')
  into v_iata_codes
  from (
    select upper(coalesce(elem->>'iata', '')) as code
    from jsonb_array_elements(
      coalesce(v_certificate.data->'canonicalAirports', '[]'::jsonb)
    ) elem
  ) sub;

  select array_agg(distinct code) filter (where code <> '')
  into v_icao_codes
  from (
    select upper(coalesce(elem->>'icao', '')) as code
    from jsonb_array_elements(
      coalesce(v_certificate.data->'canonicalAirports', '[]'::jsonb)
    ) elem
  ) sub;

  -- Also check physicalDeliveryAirportCanonical
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

  -- Reject if missing key data
  if v_coverage_month = ''
  or v_certificate_volume <= 0
  or (coalesce(cardinality(v_iata_codes), 0) = 0 and coalesce(cardinality(v_icao_codes), 0) = 0) then
    v_status := 'manual_only';
    v_match_method := 'missing-key-data';
    v_diagnostics := jsonb_build_object(
      'reason', 'Missing coverage month, volume, or airport codes',
      'coverage_month', v_coverage_month,
      'certificate_volume', v_certificate_volume,
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

  -- Validate import is active
  if p_import_id is null then
    raise exception 'An active invoice import is required for supported_simple allocation';
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

  -- Validate coverage month format before to_date
  if v_coverage_month !~ '^\d{4}-\d{2}$' then
    v_status := 'manual_only';
    v_match_method := 'invalid-coverage-month';
    v_diagnostics := jsonb_build_object(
      'reason', format('Coverage month "%s" is not in YYYY-MM format', v_coverage_month)
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

  v_month_start := to_date(v_coverage_month || '-01', 'YYYY-MM-DD');
  v_month_end := (v_month_start + interval '1 month' - interval '1 day')::date;
  v_remaining_volume := v_certificate_volume;

  -- Count total matching rows (before volume filtering) to distinguish
  -- "no rows exist" from "rows exist but fully consumed"
  select count(*)
  into v_total_row_count
  from public.invoice_rows ir
  where ir.import_id = p_import_id
    and ir.uplift_date >= v_month_start
    and ir.uplift_date <= v_month_end
    and (
      (coalesce(cardinality(v_iata_codes), 0) > 0 and upper(coalesce(ir.iata, '')) = any(v_iata_codes))
      or (coalesce(cardinality(v_icao_codes), 0) > 0 and upper(coalesce(ir.icao, '')) = any(v_icao_codes))
    );

  -- FIFO candidate selection
  for v_row in
    with locked_candidates as (
      select
        ir.id,
        ir.row_number,
        ir.invoice_no,
        ir.customer,
        ir.uplift_date,
        ir.iata,
        ir.icao,
        coalesce(ir.saf_vol_m3, 0)::numeric(18,6) as saf_vol_m3
      from public.invoice_rows ir
      where ir.import_id = p_import_id
        and ir.uplift_date >= v_month_start
        and ir.uplift_date <= v_month_end
        and (
          (coalesce(cardinality(v_iata_codes), 0) > 0 and upper(coalesce(ir.iata, '')) = any(v_iata_codes))
          or (coalesce(cardinality(v_icao_codes), 0) > 0 and upper(coalesce(ir.icao, '')) = any(v_icao_codes))
        )
      order by ir.uplift_date asc, coalesce(ir.invoice_no, '') asc, coalesce(ir.row_number, 0) asc, ir.id asc
      for update
    )
    select
      lc.*,
      lc.saf_vol_m3 - coalesce(
        (select sum(cil.allocated_m3)
         from public.certificate_invoice_links cil
         where cil.invoice_row_id = lc.id),
        0
      ) as remaining_saf_m3
    from locked_candidates lc
  loop
    if v_row.remaining_saf_m3 <= 0.000001 then
      continue;
    end if;
    v_candidate_count := v_candidate_count + 1;
    v_available_volume := round(v_available_volume + v_row.remaining_saf_m3, 6);

    v_take := least(v_remaining_volume, v_row.remaining_saf_m3);
    v_take := round(v_take, 6);

    if v_take <= 0 then
      continue;
    end if;

    v_linked_rows := v_linked_rows || jsonb_build_object(
      'invoice_row_id', v_row.id,
      'row_number', v_row.row_number,
      'invoice_no', v_row.invoice_no,
      'customer', v_row.customer,
      'uplift_date', v_row.uplift_date,
      'iata', v_row.iata,
      'icao', v_row.icao,
      'allocated_m3', v_take
    );

    v_allocated_volume := round(v_allocated_volume + v_take, 6);
    v_remaining_volume := round(greatest(0, v_certificate_volume - v_allocated_volume), 6);

    if v_remaining_volume <= v_tolerance then
      exit;
    end if;
  end loop;

  -- Determine status: auto_linked (full), partial_linked (some), unmatched (none)
  if v_allocated_volume + v_tolerance >= v_certificate_volume then
    v_status := 'auto_linked';
    v_match_method := 'fifo-monthly-partial';
  elsif v_allocated_volume > 0 then
    v_status := 'partial_linked';
    v_match_method := 'fifo-monthly-partial';
  else
    v_status := 'unmatched';
    v_match_method := 'fifo-monthly-partial';
  end if;

  v_diagnostics := jsonb_build_object(
    'coverage_month', v_coverage_month,
    'month_start', v_month_start,
    'month_end', v_month_end,
    'iata_codes', to_jsonb(coalesce(v_iata_codes, array[]::text[])),
    'icao_codes', to_jsonb(coalesce(v_icao_codes, array[]::text[])),
    'certificate_volume', v_certificate_volume,
    'allocated_volume', v_allocated_volume,
    'available_volume', v_available_volume,
    'candidate_count', v_candidate_count,
    'total_row_count', v_total_row_count,
    'linked_count', jsonb_array_length(v_linked_rows),
    'variance', round(v_certificate_volume - v_allocated_volume, 6),
    'actor', coalesce(p_actor, 'system')
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

  -- Insert links for both auto_linked AND partial_linked
  if v_status in ('auto_linked', 'partial_linked') then
    insert into public.certificate_invoice_links (
      id, certificate_match_id, certificate_id, invoice_row_id,
      row_number, invoice_no, customer, uplift_date, iata, icao,
      allocated_m3, created_at
    )
    select
      gen_random_uuid(),
      v_match_id,
      p_certificate_id,
      (item->>'invoice_row_id')::uuid,
      (item->>'row_number')::int,
      item->>'invoice_no',
      item->>'customer',
      (item->>'uplift_date')::date,
      item->>'iata',
      item->>'icao',
      (item->>'allocated_m3')::numeric(18,6),
      now()
    from jsonb_array_elements(v_linked_rows) item;
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
$$;
