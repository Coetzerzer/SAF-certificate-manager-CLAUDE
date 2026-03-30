alter table public.invoice_rows
  add column if not exists saf_vol_m3 numeric(18,6);

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_invoice_row_id_key'
  ) then
    alter table public.certificate_invoice_links
      drop constraint certificate_invoice_links_invoice_row_id_key;
  end if;
end $$;

drop index if exists public.certificate_invoice_links_invoice_row_id_idx;
create index if not exists certificate_invoice_links_invoice_row_id_idx
  on public.certificate_invoice_links(invoice_row_id);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_allocated_m3_positive_chk'
  ) then
    alter table public.certificate_invoice_links
      add constraint certificate_invoice_links_allocated_m3_positive_chk
      check (allocated_m3 is not null and allocated_m3 > 0);
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from public.certificate_invoice_links
    group by certificate_id, invoice_row_id
    having count(*) > 1
  ) then
    raise exception 'Duplicate certificate_invoice_links rows exist for the same certificate_id/invoice_row_id pair. Clean them before applying the unique index.';
  end if;
end $$;

create unique index if not exists certificate_invoice_links_certificate_row_idx
  on public.certificate_invoice_links(certificate_id, invoice_row_id);

create or replace function public.parse_flexible_numeric(p_value text)
returns numeric
language plpgsql
immutable
set search_path = public, pg_temp
as $$
declare
  v_clean text;
begin
  if p_value is null then
    return null;
  end if;

  v_clean := regexp_replace(replace(trim(p_value), ',', '.'), '[^0-9.\-]+', '', 'g');
  if v_clean in ('', '-', '.', '-.') then
    return null;
  end if;

  begin
    return v_clean::numeric;
  exception
    when others then
      return null;
  end;
end;
$$;

create or replace function public.enforce_safe_certificate_link_allocation()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_invoice_capacity numeric(18,6);
  v_invoice_allocated numeric(18,6);
  v_certificate_capacity numeric(18,6);
  v_certificate_allocated numeric(18,6);
begin
  if tg_op = 'DELETE' then
    return old;
  end if;

  if new.certificate_id is null or new.invoice_row_id is null then
    raise exception 'certificate_id and invoice_row_id are required for certificate_invoice_links';
  end if;

  if new.allocated_m3 is null or new.allocated_m3 <= 0 then
    raise exception 'allocated_m3 must be greater than 0';
  end if;

  perform 1
  from public.certificates
  where id = new.certificate_id
  for update;

  if not found then
    raise exception 'Certificate % does not exist', new.certificate_id;
  end if;

  perform 1
  from public.invoice_rows
  where id = new.invoice_row_id
  for update;

  if not found then
    raise exception 'Invoice row % does not exist', new.invoice_row_id;
  end if;

  select coalesce(saf_vol_m3, 0)
  into v_invoice_capacity
  from public.invoice_rows
  where id = new.invoice_row_id;

  select coalesce(sum(allocated_m3), 0)
  into v_invoice_allocated
  from public.certificate_invoice_links
  where invoice_row_id = new.invoice_row_id
    and (new.id is null or id <> new.id);

  if v_invoice_allocated + new.allocated_m3 > v_invoice_capacity + 0.000001 then
    raise exception 'Invoice row % would be over-allocated: attempted %, available %',
      new.invoice_row_id,
      round(v_invoice_allocated + new.allocated_m3, 6),
      round(v_invoice_capacity, 6);
  end if;

  select public.parse_flexible_numeric(coalesce(data->>'quantity', ''))
  into v_certificate_capacity
  from public.certificates
  where id = new.certificate_id;

  if v_certificate_capacity is null or v_certificate_capacity <= 0 then
    raise exception 'Certificate % has no valid SAF quantity', new.certificate_id;
  end if;

  select coalesce(sum(allocated_m3), 0)
  into v_certificate_allocated
  from public.certificate_invoice_links
  where certificate_id = new.certificate_id
    and (new.id is null or id <> new.id);

  if v_certificate_allocated + new.allocated_m3 > v_certificate_capacity + 0.000001 then
    raise exception 'Certificate % would be over-allocated: attempted %, capacity %',
      new.certificate_id,
      round(v_certificate_allocated + new.allocated_m3, 6),
      round(v_certificate_capacity, 6);
  end if;

  return new;
end;
$$;

drop trigger if exists trg_enforce_safe_certificate_link_allocation on public.certificate_invoice_links;
create trigger trg_enforce_safe_certificate_link_allocation
before insert or update of certificate_id, invoice_row_id, allocated_m3
on public.certificate_invoice_links
for each row
execute function public.enforce_safe_certificate_link_allocation();

create or replace function public.allocate_simple_certificate(
  p_certificate_id uuid,
  p_import_id uuid,
  p_actor text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_certificate public.certificates%rowtype;
  v_document_family text;
  v_matching_mode text;
  v_support_reason text;
  v_certificate_volume numeric(18,6);
  v_coverage_month text;
  v_month_start date;
  v_month_end date;
  v_iata_codes text[] := '{}'::text[];
  v_icao_codes text[] := '{}'::text[];
  v_linked_rows jsonb := '[]'::jsonb;
  v_match_id uuid;
  v_status text;
  v_match_method text;
  v_review_note text;
  v_allocated_volume numeric(18,6) := 0;
  v_remaining_volume numeric(18,6) := 0;
  v_available_volume numeric(18,6) := 0;
  v_row_count integer := 0;
  v_take numeric(18,6);
  v_row record;
  v_diagnostics jsonb;
begin
  if auth.uid() is null then
    raise exception 'Authentication required' using errcode = '42501';
  end if;

  if p_certificate_id is null then
    raise exception 'p_certificate_id is required';
  end if;

  select *
  into v_certificate
  from public.certificates
  where id = p_certificate_id
  for update;

  if not found then
    raise exception 'Certificate % was not found', p_certificate_id;
  end if;

  v_document_family := coalesce(v_certificate.document_family, v_certificate.data->>'document_family', 'manual_only');
  v_matching_mode := coalesce(v_certificate.matching_mode, v_certificate.data->>'matching_mode', v_certificate.data->>'matchingMode', 'manual_only');
  v_support_reason := coalesce(
    nullif(v_certificate.data->>'support_reason', ''),
    nullif(v_certificate.classification_reason, ''),
    'Certificate is outside the supported simple scope and will not be auto-linked.'
  );
  v_certificate_volume := public.parse_flexible_numeric(coalesce(v_certificate.data->>'quantity', ''));
  v_coverage_month := nullif(coalesce(v_certificate.data->>'coverageMonth', ''), '');

  select coalesce(array_agg(distinct code) filter (where code <> ''), '{}'::text[])
  into v_iata_codes
  from (
    select upper(coalesce(elem->>'iata', '')) as code
    from jsonb_array_elements(coalesce(v_certificate.data->'canonicalAirports', '[]'::jsonb)) elem
    union all
    select upper(coalesce(v_certificate.data #>> '{physicalDeliveryAirportCanonical,iata}', ''))
  ) codes;

  select coalesce(array_agg(distinct code) filter (where code <> ''), '{}'::text[])
  into v_icao_codes
  from (
    select upper(coalesce(elem->>'icao', '')) as code
    from jsonb_array_elements(coalesce(v_certificate.data->'canonicalAirports', '[]'::jsonb)) elem
    union all
    select upper(coalesce(v_certificate.data #>> '{physicalDeliveryAirportCanonical,icao}', ''))
  ) codes;

  delete from public.certificate_matches
  where certificate_id = p_certificate_id;

  if v_document_family <> 'supported_simple'
     or v_matching_mode <> 'simple_monthly_airport'
     or v_coverage_month is null
     or v_certificate_volume is null
     or v_certificate_volume <= 0
     or coalesce(cardinality(v_iata_codes), 0) + coalesce(cardinality(v_icao_codes), 0) = 0 then
    v_status := 'manual_only';
    v_match_method := 'manual_only';
    v_review_note := v_support_reason;
    v_diagnostics := jsonb_build_object(
      'matching_path', 'manual_only',
      'reason', v_review_note,
      'coverage_month', coalesce(v_coverage_month, ''),
      'iata_codes', to_jsonb(v_iata_codes),
      'icao_codes', to_jsonb(v_icao_codes)
    );

    insert into public.certificate_matches (
      certificate_id,
      status,
      match_method,
      cert_volume_m3,
      allocated_volume_m3,
      variance_m3,
      review_note,
      reviewed_by,
      reviewed_at,
      candidate_sets,
      diagnostics,
      created_at,
      updated_at
    )
    values (
      p_certificate_id,
      v_status,
      v_match_method,
      v_certificate_volume,
      0,
      v_certificate_volume,
      v_review_note,
      p_actor,
      null,
      '[]'::jsonb,
      v_diagnostics,
      now(),
      now()
    )
    returning id into v_match_id;

    return jsonb_build_object(
      'certificate_id', p_certificate_id,
      'certificate_match_id', v_match_id,
      'status', v_status,
      'match_method', v_match_method,
      'cert_volume_m3', v_certificate_volume,
      'allocated_volume_m3', 0,
      'remaining_volume_m3', v_certificate_volume,
      'variance_m3', v_certificate_volume,
      'review_note', v_review_note,
      'candidate_sets', '[]'::jsonb,
      'linked_rows', '[]'::jsonb,
      'diagnostics', v_diagnostics
    );
  end if;

  if p_import_id is null then
    raise exception 'An active invoice import is required for supported_simple allocation';
  end if;

  perform 1
  from public.invoice_imports
  where id = p_import_id;

  if not found then
    raise exception 'Invoice import % was not found', p_import_id;
  end if;

  v_month_start := to_date(v_coverage_month || '-01', 'YYYY-MM-DD');
  v_month_end := (v_month_start + interval '1 month' - interval '1 day')::date;
  v_remaining_volume := v_certificate_volume;

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
      candidate.*,
      greatest(0::numeric, candidate.saf_vol_m3 - coalesce(links.allocated_m3, 0))::numeric(18,6) as remaining_saf_m3
    from locked_candidates candidate
    left join (
      select invoice_row_id, coalesce(sum(allocated_m3), 0)::numeric(18,6) as allocated_m3
      from public.certificate_invoice_links
      group by invoice_row_id
    ) links on links.invoice_row_id = candidate.id
    where greatest(0::numeric, candidate.saf_vol_m3 - coalesce(links.allocated_m3, 0)) > 0
    order by candidate.uplift_date asc, coalesce(candidate.invoice_no, '') asc, coalesce(candidate.row_number, 0) asc, candidate.id asc
  loop
    v_row_count := v_row_count + 1;
    v_available_volume := round(v_available_volume + v_row.remaining_saf_m3, 6);

    if v_remaining_volume <= 0.000001 then
      continue;
    end if;

    v_take := round(least(v_remaining_volume, v_row.remaining_saf_m3), 6);
    if v_take <= 0.000001 then
      continue;
    end if;

    v_linked_rows := v_linked_rows || jsonb_build_array(
      jsonb_build_object(
        'invoice_row_id', v_row.id,
        'row_number', v_row.row_number,
        'invoice_no', coalesce(v_row.invoice_no, ''),
        'customer', coalesce(v_row.customer, ''),
        'uplift_date', v_row.uplift_date,
        'iata', coalesce(v_row.iata, ''),
        'icao', coalesce(v_row.icao, ''),
        'allocated_m3', v_take
      )
    );

    v_allocated_volume := round(v_allocated_volume + v_take, 6);
    v_remaining_volume := round(greatest(0, v_certificate_volume - v_allocated_volume), 6);
  end loop;

  if v_allocated_volume + 0.000001 < v_certificate_volume then
    v_status := 'unmatched';
    v_match_method := 'simple_monthly_airport';
    v_review_note := format(
      'Only %s m3 remains available for %s within the %s airport-month pool, below the certificate target of %s m3.',
      round(v_available_volume, 6),
      coalesce(array_to_string(v_iata_codes, '/'), array_to_string(v_icao_codes, '/')),
      v_coverage_month,
      round(v_certificate_volume, 6)
    );
    v_linked_rows := '[]'::jsonb;
    v_allocated_volume := 0;
    v_remaining_volume := v_certificate_volume;
  else
    v_status := 'auto_linked';
    v_match_method := 'fifo-monthly-partial';
    v_review_note := format(
      'Matched by deterministic FIFO allocation for %s %s. Rows are filtered by airport and month, then consumed by uplift date with partial final-row allocation when needed.',
      coalesce(array_to_string(v_iata_codes, '/'), array_to_string(v_icao_codes, '/')),
      v_coverage_month
    );
  end if;

  v_diagnostics := jsonb_build_object(
    'matching_path', 'simple_monthly_airport',
    'allocation_policy', 'fifo-airport-month-partial',
    'invoice_import_id', p_import_id,
    'coverage_month', v_coverage_month,
    'iata_codes', to_jsonb(v_iata_codes),
    'icao_codes', to_jsonb(v_icao_codes),
    'candidate_row_count', v_row_count,
    'available_volume_m3', v_available_volume,
    'allocated_row_count', jsonb_array_length(v_linked_rows)
  );

  insert into public.certificate_matches (
    certificate_id,
    status,
    match_method,
    cert_volume_m3,
    allocated_volume_m3,
    variance_m3,
    review_note,
    reviewed_by,
    reviewed_at,
    candidate_sets,
    diagnostics,
    created_at,
    updated_at
  )
  values (
    p_certificate_id,
    v_status,
    v_match_method,
    v_certificate_volume,
    v_allocated_volume,
    round(v_allocated_volume - v_certificate_volume, 6),
    v_review_note,
    p_actor,
    null,
    case
      when v_status = 'auto_linked' then jsonb_build_array(
        jsonb_build_object(
          'key', (
            select string_agg(
              format('%s:%s', item->>'invoice_row_id', item->>'allocated_m3'),
              '|'
              order by item->>'uplift_date', item->>'invoice_no', item->>'row_number'
            )
            from jsonb_array_elements(v_linked_rows) item
          ),
          'match_method', v_match_method,
          'total_volume_m3', v_allocated_volume,
          'variance_m3', round(v_allocated_volume - v_certificate_volume, 6),
          'rows', v_linked_rows,
          'groups', '[]'::jsonb,
          'reason', 'fifo-monthly-partial',
          'score', 1000 - jsonb_array_length(v_linked_rows)
        )
      )
      else '[]'::jsonb
    end,
    v_diagnostics,
    now(),
    now()
  )
  returning id into v_match_id;

  if v_status = 'auto_linked' then
    insert into public.certificate_invoice_links (
      certificate_match_id,
      certificate_id,
      invoice_row_id,
      row_number,
      invoice_no,
      customer,
      uplift_date,
      iata,
      icao,
      allocated_m3,
      created_at
    )
    select
      v_match_id,
      p_certificate_id,
      (item->>'invoice_row_id')::uuid,
      nullif(item->>'row_number', '')::integer,
      nullif(item->>'invoice_no', ''),
      nullif(item->>'customer', ''),
      nullif(item->>'uplift_date', '')::date,
      nullif(item->>'iata', ''),
      nullif(item->>'icao', ''),
      (item->>'allocated_m3')::numeric(18,6),
      now()
    from jsonb_array_elements(v_linked_rows) item;
  end if;

  return jsonb_build_object(
    'certificate_id', p_certificate_id,
    'certificate_match_id', v_match_id,
    'status', v_status,
    'match_method', v_match_method,
    'cert_volume_m3', v_certificate_volume,
    'allocated_volume_m3', v_allocated_volume,
    'remaining_volume_m3', v_remaining_volume,
    'variance_m3', round(v_allocated_volume - v_certificate_volume, 6),
    'review_note', v_review_note,
    'candidate_sets',
      case
        when v_status = 'auto_linked' then jsonb_build_array(
          jsonb_build_object(
            'match_method', v_match_method,
            'total_volume_m3', v_allocated_volume,
            'variance_m3', round(v_allocated_volume - v_certificate_volume, 6),
            'rows', v_linked_rows,
            'groups', '[]'::jsonb,
            'reason', 'fifo-monthly-partial',
            'score', 1000 - jsonb_array_length(v_linked_rows)
          )
        )
        else '[]'::jsonb
      end,
    'linked_rows', v_linked_rows,
    'diagnostics', v_diagnostics
  );
end;
$$;

revoke all on function public.allocate_simple_certificate(uuid, uuid, text) from public;
grant execute on function public.allocate_simple_certificate(uuid, uuid, text) to authenticated;
