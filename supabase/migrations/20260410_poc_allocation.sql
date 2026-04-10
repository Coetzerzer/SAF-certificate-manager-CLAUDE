-- Migration: Add allocate_poc_certificate() for multi-airport PoC certificates
-- Supports document_family = 'supported_poc', matching_mode = 'poc_monthly_airport'
-- Runs FIFO per allocation unit (one unit per airport-month from monthlyVolumes)

create or replace function public.allocate_poc_certificate(
  p_certificate_id uuid,
  p_import_id      uuid    default null,
  p_actor          text    default null
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_certificate        record;
  v_certificate_volume numeric(18,6);
  v_import_status      text;
  v_tolerance          numeric(18,6) := 0.002;

  -- Per-unit iteration
  v_unit               record;
  v_unit_iata_codes    text[];
  v_unit_icao_codes    text[];
  v_unit_remaining     numeric(18,6);
  v_unit_allocated     numeric(18,6);
  v_unit_available     numeric(18,6);
  v_unit_candidate_cnt int;
  v_unit_total_row_cnt int;
  v_unit_status        text;

  -- Invoice row iteration
  v_row                record;
  v_take               numeric(18,6);
  v_unit_linked_rows   jsonb;

  -- Aggregates
  v_total_allocated    numeric(18,6) := 0;
  v_match_id           uuid;
  v_overall_status     text;
  v_unit_breakdown     jsonb := '[]'::jsonb;
  v_all_linked_rows    jsonb := '[]'::jsonb;
  v_diagnostics        jsonb;
begin
  -- Lock and fetch certificate
  select *
  into v_certificate
  from public.certificates
  where id = p_certificate_id
  for update;

  if not found then
    raise exception 'Certificate % was not found', p_certificate_id;
  end if;

  -- Guard: must be supported_poc / poc_monthly_airport
  if coalesce(v_certificate.document_family, '') <> 'supported_poc'
  or coalesce(v_certificate.matching_mode, '') <> 'poc_monthly_airport' then
    raise exception
      'allocate_poc_certificate requires document_family=supported_poc and matching_mode=poc_monthly_airport, got %/%',
      v_certificate.document_family, v_certificate.matching_mode;
  end if;

  -- Extract total cert volume
  v_certificate_volume := coalesce(
    public.parse_flexible_numeric(v_certificate.data->>'quantity'), 0
  );

  -- Validate import
  if p_import_id is null then
    raise exception 'An active invoice import is required for poc_monthly_airport allocation';
  end if;

  select status into v_import_status
  from public.invoice_imports
  where id = p_import_id;

  if not found then
    raise exception 'Invoice import % was not found', p_import_id;
  end if;

  if v_import_status <> 'active' then
    raise exception
      'Invoice import % has status "%" — only active imports can be used for allocation',
      p_import_id, v_import_status;
  end if;

  -- Wipe prior links and match for this certificate
  delete from public.certificate_invoice_links
  where certificate_id = p_certificate_id;

  delete from public.certificate_matches
  where certificate_id = p_certificate_id;

  -- Reserve a match_id shared by all links
  v_match_id := gen_random_uuid();

  -- Loop over allocation units ordered by unit_index
  for v_unit in
    select *
    from public.certificate_allocation_units
    where certificate_id = p_certificate_id
      and coalesce(matching_mode_override, '') = 'poc_monthly_airport'
    order by unit_index asc
  loop
    v_unit_linked_rows   := '[]'::jsonb;
    v_unit_allocated     := 0;
    v_unit_available     := 0;
    v_unit_candidate_cnt := 0;
    v_unit_total_row_cnt := 0;
    v_unit_remaining     := coalesce(v_unit.saf_volume_m3, 0);

    -- Build airport code arrays for this unit
    v_unit_iata_codes := case
      when coalesce(v_unit.airport_iata, '') <> '' then array[upper(v_unit.airport_iata)]
      else array[]::text[]
    end;
    v_unit_icao_codes := case
      when coalesce(v_unit.airport_icao, '') <> '' then array[upper(v_unit.airport_icao)]
      else array[]::text[]
    end;

    -- Skip unit if no airport codes, no period, or zero volume
    if coalesce(cardinality(v_unit_iata_codes), 0) + coalesce(cardinality(v_unit_icao_codes), 0) = 0
    or v_unit.period_start is null
    or v_unit.period_end is null
    or v_unit_remaining <= 0 then
      v_unit_status := 'skipped';
      v_unit_breakdown := v_unit_breakdown || jsonb_build_object(
        'unit_index',          v_unit.unit_index,
        'airport_iata',        v_unit.airport_iata,
        'airport_icao',        v_unit.airport_icao,
        'airport_name',        v_unit.airport_name,
        'period_start',        v_unit.period_start,
        'period_end',          v_unit.period_end,
        'target_volume_m3',    v_unit.saf_volume_m3,
        'allocated_volume_m3', 0,
        'available_volume_m3', 0,
        'candidate_count',     0,
        'total_row_count',     0,
        'linked_count',        0,
        'status',              v_unit_status,
        'reason',              'Missing airport codes, period, or zero volume'
      );
      continue;
    end if;

    -- Count total candidate rows for this unit (diagnostics)
    select count(*)
    into v_unit_total_row_cnt
    from public.invoice_rows ir
    where ir.import_id = p_import_id
      and ir.uplift_date >= v_unit.period_start
      and ir.uplift_date <= v_unit.period_end
      and (
        (cardinality(v_unit_iata_codes) > 0
          and upper(coalesce(ir.iata, '')) = any(v_unit_iata_codes))
        or
        (cardinality(v_unit_icao_codes) > 0
          and upper(coalesce(ir.icao, '')) = any(v_unit_icao_codes))
      );

    -- FIFO over this unit's airport+period slice
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
          and ir.uplift_date >= v_unit.period_start
          and ir.uplift_date <= v_unit.period_end
          and (
            (cardinality(v_unit_iata_codes) > 0
              and upper(coalesce(ir.iata, '')) = any(v_unit_iata_codes))
            or
            (cardinality(v_unit_icao_codes) > 0
              and upper(coalesce(ir.icao, '')) = any(v_unit_icao_codes))
          )
        order by ir.uplift_date asc, coalesce(ir.invoice_no, '') asc,
                 coalesce(ir.row_number, 0) asc, ir.id asc
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

      v_unit_candidate_cnt := v_unit_candidate_cnt + 1;
      v_unit_available := round(v_unit_available + v_row.remaining_saf_m3, 6);

      v_take := round(least(v_unit_remaining, v_row.remaining_saf_m3), 6);
      if v_take <= 0 then
        continue;
      end if;

      v_unit_linked_rows := v_unit_linked_rows || jsonb_build_object(
        'invoice_row_id',        v_row.id,
        'row_number',            v_row.row_number,
        'invoice_no',            v_row.invoice_no,
        'customer',              v_row.customer,
        'uplift_date',           v_row.uplift_date,
        'iata',                  v_row.iata,
        'icao',                  v_row.icao,
        'allocated_m3',          v_take,
        'allocation_unit_id',    v_unit.id,
        'allocation_unit_index', v_unit.unit_index,
        'allocation_unit_type',  v_unit.unit_type
      );

      v_unit_allocated  := round(v_unit_allocated + v_take, 6);
      v_unit_remaining  := round(greatest(0, v_unit.saf_volume_m3 - v_unit_allocated), 6);

      if v_unit_remaining <= v_tolerance then
        exit;
      end if;
    end loop;

    -- Per-unit status
    if v_unit_allocated + v_tolerance >= coalesce(v_unit.saf_volume_m3, 0) then
      v_unit_status := 'auto_linked';
    elsif v_unit_allocated > 0 then
      v_unit_status := 'partial_linked';
    else
      v_unit_status := 'unmatched';
    end if;

    -- Update allocation unit consumption counters
    update public.certificate_allocation_units
    set consumed_volume_m3  = v_unit_allocated,
        remaining_volume_m3 = greatest(0, coalesce(saf_volume_m3, 0) - v_unit_allocated),
        updated_at          = now()
    where id = v_unit.id;

    -- Accumulate for aggregate result
    v_total_allocated := round(v_total_allocated + v_unit_allocated, 6);
    v_all_linked_rows := v_all_linked_rows || v_unit_linked_rows;

    v_unit_breakdown := v_unit_breakdown || jsonb_build_object(
      'unit_index',          v_unit.unit_index,
      'airport_iata',        v_unit.airport_iata,
      'airport_icao',        v_unit.airport_icao,
      'airport_name',        v_unit.airport_name,
      'period_start',        v_unit.period_start,
      'period_end',          v_unit.period_end,
      'target_volume_m3',    v_unit.saf_volume_m3,
      'allocated_volume_m3', v_unit_allocated,
      'available_volume_m3', v_unit_available,
      'candidate_count',     v_unit_candidate_cnt,
      'total_row_count',     v_unit_total_row_cnt,
      'linked_count',        jsonb_array_length(v_unit_linked_rows),
      'status',              v_unit_status
    );
  end loop;

  -- Overall status
  if v_total_allocated + v_tolerance >= v_certificate_volume then
    v_overall_status := 'auto_linked';
  elsif v_total_allocated > 0 then
    v_overall_status := 'partial_linked';
  else
    v_overall_status := 'unmatched';
  end if;

  v_diagnostics := jsonb_build_object(
    'matching_path',      'poc_monthly_airport',
    'allocation_policy',  'fifo-airport-month-partial-per-unit',
    'invoice_import_id',  p_import_id,
    'certificate_volume', v_certificate_volume,
    'allocated_volume',   v_total_allocated,
    'variance',           round(v_certificate_volume - v_total_allocated, 6),
    'unit_count',         jsonb_array_length(v_unit_breakdown),
    'linked_count',       jsonb_array_length(v_all_linked_rows),
    'actor',              coalesce(p_actor, 'system'),
    'unit_breakdown',     v_unit_breakdown
  );

  -- Upsert aggregate certificate_matches row
  insert into public.certificate_matches (
    id, certificate_id, status, match_method,
    cert_volume_m3, allocated_volume_m3, variance_m3,
    diagnostics, created_at, updated_at
  ) values (
    v_match_id,
    p_certificate_id,
    v_overall_status,
    'fifo-poc-monthly-airport',
    v_certificate_volume,
    v_total_allocated,
    round(v_certificate_volume - v_total_allocated, 6),
    v_diagnostics,
    now(),
    now()
  );

  -- Insert all links (with allocation_unit_id per link)
  if jsonb_array_length(v_all_linked_rows) > 0 then
    insert into public.certificate_invoice_links (
      id, certificate_match_id, certificate_id, invoice_row_id,
      row_number, invoice_no, customer, uplift_date, iata, icao,
      allocated_m3, allocation_unit_id, allocation_unit_index, allocation_unit_type,
      created_at
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
      (item->>'allocation_unit_id')::uuid,
      (item->>'allocation_unit_index')::int,
      item->>'allocation_unit_type',
      now()
    from jsonb_array_elements(v_all_linked_rows) item;
  end if;

  return jsonb_build_object(
    'certificate_id',        p_certificate_id,
    'certificate_match_id',  v_match_id,
    'status',                v_overall_status,
    'match_method',          'fifo-poc-monthly-airport',
    'cert_volume_m3',        v_certificate_volume,
    'allocated_volume_m3',   v_total_allocated,
    'variance_m3',           round(v_certificate_volume - v_total_allocated, 6),
    'linked_count',          jsonb_array_length(v_all_linked_rows),
    'unit_breakdown',        v_unit_breakdown,
    'diagnostics',           v_diagnostics
  );
end;
$$;

revoke all on function public.allocate_poc_certificate(uuid, uuid, text) from public;
grant execute on function public.allocate_poc_certificate(uuid, uuid, text) to authenticated;
