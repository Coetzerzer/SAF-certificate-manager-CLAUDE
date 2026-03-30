-- Run this in your Supabase dashboard: https://supabase.com/dashboard/project/booddnrpwvphgurfixll/sql/new

create table if not exists certificates (
  id uuid primary key default gen_random_uuid(),
  filename text not null,
  data jsonb not null default '{}',
  analysis jsonb,
  unique_number text,
  pdf_path text,
  document_family text,
  matching_mode text,
  classification_confidence numeric(5,4),
  review_required boolean default false,
  classification_reason text,
  created_at timestamptz default now()
);

alter table certificates add column if not exists document_family text;
alter table certificates add column if not exists matching_mode text;
alter table certificates add column if not exists classification_confidence numeric(5,4);
alter table certificates add column if not exists review_required boolean default false;
alter table certificates add column if not exists classification_reason text;

-- Deduplicate by SAF unique number (null values are excluded so blanks never conflict)
create unique index if not exists certificates_unique_number_idx
  on certificates (unique_number)
  where unique_number is not null;

create index if not exists certificates_matching_mode_idx on certificates(matching_mode);
create index if not exists certificates_review_required_idx on certificates(review_required);

create table if not exists certificate_allocation_units (
  id uuid primary key default gen_random_uuid(),
  certificate_id uuid not null references certificates(id) on delete cascade,
  unit_index integer not null,
  unit_type text,
  airport_iata text,
  airport_icao text,
  airport_name text,
  period_start date,
  period_end date,
  dispatch_date date,
  saf_volume_m3 numeric(18,6),
  jet_volume_m3 numeric(18,6),
  source_reference text,
  matching_mode_override text,
  review_required boolean not null default false,
  normalization_warning text,
  consumed_volume_m3 numeric(18,6) not null default 0,
  remaining_volume_m3 numeric(18,6),
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table certificate_allocation_units add column if not exists certificate_id uuid;
alter table certificate_allocation_units add column if not exists unit_index integer;
alter table certificate_allocation_units add column if not exists unit_type text;
alter table certificate_allocation_units add column if not exists airport_iata text;
alter table certificate_allocation_units add column if not exists airport_icao text;
alter table certificate_allocation_units add column if not exists airport_name text;
alter table certificate_allocation_units add column if not exists period_start date;
alter table certificate_allocation_units add column if not exists period_end date;
alter table certificate_allocation_units add column if not exists dispatch_date date;
alter table certificate_allocation_units add column if not exists saf_volume_m3 numeric(18,6);
alter table certificate_allocation_units add column if not exists jet_volume_m3 numeric(18,6);
alter table certificate_allocation_units add column if not exists source_reference text;
alter table certificate_allocation_units add column if not exists matching_mode_override text;
alter table certificate_allocation_units add column if not exists review_required boolean default false;
alter table certificate_allocation_units add column if not exists normalization_warning text;
alter table certificate_allocation_units add column if not exists consumed_volume_m3 numeric(18,6) default 0;
alter table certificate_allocation_units add column if not exists remaining_volume_m3 numeric(18,6);
alter table certificate_allocation_units add column if not exists created_at timestamptz default now();
alter table certificate_allocation_units add column if not exists updated_at timestamptz default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_allocation_units_certificate_id_fkey'
  ) then
    alter table certificate_allocation_units
      add constraint certificate_allocation_units_certificate_id_fkey
      foreign key (certificate_id) references certificates(id) on delete cascade;
  end if;
end $$;

create unique index if not exists certificate_allocation_units_certificate_unit_idx
  on certificate_allocation_units(certificate_id, unit_index);
create index if not exists certificate_allocation_units_certificate_idx on certificate_allocation_units(certificate_id);
create index if not exists certificate_allocation_units_review_idx on certificate_allocation_units(review_required);

-- Invoices: metadata only — rows are no longer stored in DB, CSV lives in storage bucket
create table if not exists invoices (
  id uuid primary key default gen_random_uuid(),
  filename text not null,
  csv_path text,
  created_at timestamptz default now()
);

create table if not exists invoice_imports (
  id uuid primary key default gen_random_uuid(),
  filename text not null,
  storage_path text not null,
  year integer not null default 2025,
  status text not null default 'staging',
  row_count integer not null default 0,
  candidate_row_count integer not null default 0,
  invalid_row_count integer not null default 0,
  duplicate_row_count integer not null default 0,
  validation_summary jsonb not null default '{}'::jsonb,
  activated_at timestamptz,
  failed_at timestamptz,
  created_at timestamptz default now()
);

create unique index if not exists invoice_imports_active_year_idx
  on invoice_imports (year)
  where status = 'active';
create index if not exists invoice_imports_status_idx
  on invoice_imports(status, year, created_at desc);

create table if not exists invoice_rows (
  id uuid primary key default gen_random_uuid(),
  import_id uuid not null references invoice_imports(id) on delete cascade,
  row_number integer not null,
  invoice_no text,
  customer text,
  uplift_date date,
  flight_no text,
  delivery_ticket text,
  iata text,
  icao text,
  country text,
  supplier text,
  vol_m3 numeric(18,6),
  saf_vol_m3 numeric(18,6),
  raw_payload jsonb not null default '{}'::jsonb,
  is_allocated boolean not null default false,
  created_at timestamptz default now()
);

alter table invoice_imports add column if not exists filename text;
alter table invoice_imports add column if not exists storage_path text;
alter table invoice_imports add column if not exists year integer default 2025;
alter table invoice_imports add column if not exists status text default 'staging';
alter table invoice_imports add column if not exists row_count integer default 0;
alter table invoice_imports add column if not exists candidate_row_count integer default 0;
alter table invoice_imports add column if not exists invalid_row_count integer default 0;
alter table invoice_imports add column if not exists duplicate_row_count integer default 0;
alter table invoice_imports add column if not exists validation_summary jsonb default '{}'::jsonb;
alter table invoice_imports add column if not exists activated_at timestamptz;
alter table invoice_imports add column if not exists failed_at timestamptz;
alter table invoice_imports add column if not exists created_at timestamptz default now();
alter table invoice_imports alter column status set default 'staging';

alter table invoice_rows add column if not exists import_id uuid;
alter table invoice_rows add column if not exists row_number integer;
alter table invoice_rows add column if not exists invoice_no text;
alter table invoice_rows add column if not exists customer text;
alter table invoice_rows add column if not exists uplift_date date;
alter table invoice_rows add column if not exists flight_no text;
alter table invoice_rows add column if not exists delivery_ticket text;
alter table invoice_rows add column if not exists iata text;
alter table invoice_rows add column if not exists icao text;
alter table invoice_rows add column if not exists country text;
alter table invoice_rows add column if not exists supplier text;
alter table invoice_rows add column if not exists vol_m3 numeric(18,6);
alter table invoice_rows add column if not exists saf_vol_m3 numeric(18,6);
alter table invoice_rows add column if not exists raw_payload jsonb default '{}'::jsonb;
alter table invoice_rows add column if not exists is_allocated boolean default false;
alter table invoice_rows add column if not exists is_duplicate boolean default false;
alter table invoice_rows add column if not exists duplicate_group_key text;
alter table invoice_rows add column if not exists validation_note text;
alter table invoice_rows add column if not exists created_at timestamptz default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'invoice_imports_status_valid_chk'
  ) then
    alter table invoice_imports
      add constraint invoice_imports_status_valid_chk
      check (status in ('staging', 'active', 'failed', 'superseded'));
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'invoice_rows'
      and column_name = 'invoice_file_id'
  ) then
    execute 'alter table invoice_rows alter column invoice_file_id drop not null';
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'invoice_rows_import_id_fkey'
  ) then
    alter table invoice_rows
      add constraint invoice_rows_import_id_fkey
      foreign key (import_id) references invoice_imports(id) on delete cascade;
  end if;
end $$;

create index if not exists invoice_rows_import_idx on invoice_rows(import_id);
create index if not exists invoice_rows_allocated_idx on invoice_rows(is_allocated);
create index if not exists invoice_rows_invoice_no_idx on invoice_rows(invoice_no);
create index if not exists invoice_rows_airport_idx on invoice_rows(iata, icao);
create index if not exists invoice_rows_import_duplicate_idx on invoice_rows(import_id, is_duplicate, duplicate_group_key);

create table if not exists client_certificates (
  id uuid primary key default gen_random_uuid(),
  group_key text not null,
  client_name text not null,
  airport_code text not null,
  month text not null,
  total_saf_volume_m3 numeric(18,6) not null default 0,
  source_certificate_refs jsonb not null default '[]'::jsonb,
  source_certificate_ids jsonb not null default '[]'::jsonb,
  source_invoice_row_ids jsonb not null default '[]'::jsonb,
  source_link_ids jsonb not null default '[]'::jsonb,
  approved_link_count integer not null default 0,
  matched_row_count integer not null default 0,
  issue_date date,
  internal_reference text not null,
  generated_file_path text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table client_certificates add column if not exists group_key text;
alter table client_certificates add column if not exists client_name text;
alter table client_certificates add column if not exists airport_code text;
alter table client_certificates add column if not exists month text;
alter table client_certificates add column if not exists total_saf_volume_m3 numeric(18,6) default 0;
alter table client_certificates add column if not exists source_certificate_refs jsonb default '[]'::jsonb;
alter table client_certificates add column if not exists source_certificate_ids jsonb default '[]'::jsonb;
alter table client_certificates add column if not exists source_invoice_row_ids jsonb default '[]'::jsonb;
alter table client_certificates add column if not exists source_link_ids jsonb default '[]'::jsonb;
alter table client_certificates add column if not exists approved_link_count integer default 0;
alter table client_certificates add column if not exists matched_row_count integer default 0;
alter table client_certificates add column if not exists issue_date date;
alter table client_certificates add column if not exists internal_reference text;
alter table client_certificates add column if not exists generated_file_path text;
alter table client_certificates add column if not exists created_at timestamptz default now();
alter table client_certificates add column if not exists updated_at timestamptz default now();

create unique index if not exists client_certificates_group_key_idx on client_certificates(group_key);
create unique index if not exists client_certificates_internal_reference_idx on client_certificates(internal_reference);
create index if not exists client_certificates_client_month_idx on client_certificates(client_name, airport_code, month);

create or replace function public.activate_invoice_import(p_import_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_target public.invoice_imports%rowtype;
  v_previous_active_id uuid;
  v_previous_active_filename text;
begin
  if auth.uid() is null then
    raise exception 'Authentication required' using errcode = '42501';
  end if;

  if p_import_id is null then
    raise exception 'p_import_id is required';
  end if;

  select *
  into v_target
  from public.invoice_imports
  where id = p_import_id
  for update;

  if not found then
    raise exception 'Invoice import % was not found', p_import_id;
  end if;

  if v_target.status <> 'staging' then
    raise exception 'Invoice import % must be staging before activation (current status: %)', p_import_id, v_target.status;
  end if;

  select id, filename
  into v_previous_active_id, v_previous_active_filename
  from public.invoice_imports
  where year = v_target.year
    and status = 'active'
    and id <> p_import_id
  order by activated_at desc nulls last, created_at desc
  limit 1
  for update;

  update public.invoice_imports
  set status = 'superseded'
  where year = v_target.year
    and status = 'active'
    and id <> p_import_id;

  update public.invoice_imports
  set status = 'active',
      activated_at = now(),
      failed_at = null
  where id = p_import_id;

  delete from public.certificate_matches;

  update public.certificate_allocation_units
  set consumed_volume_m3 = 0,
      remaining_volume_m3 = case
        when saf_volume_m3 is null then null
        else greatest(0, saf_volume_m3)
      end,
      updated_at = now();

  return jsonb_build_object(
    'activated_import_id', p_import_id,
    'activated_filename', v_target.filename,
    'year', v_target.year,
    'previous_active_import_id', v_previous_active_id,
    'previous_active_filename', v_previous_active_filename,
    'candidate_row_count', v_target.candidate_row_count,
    'duplicate_row_count', v_target.duplicate_row_count
  );
end;
$$;

revoke all on function public.activate_invoice_import(uuid) from public;
grant execute on function public.activate_invoice_import(uuid) to authenticated;

create table if not exists certificate_matches (
  id uuid primary key default gen_random_uuid(),
  certificate_id uuid not null unique references certificates(id) on delete cascade,
  status text not null,
  match_method text,
  cert_volume_m3 numeric(18,6),
  allocated_volume_m3 numeric(18,6),
  variance_m3 numeric(18,6),
  review_note text,
  reviewed_by text,
  reviewed_at timestamptz,
  candidate_sets jsonb not null default '[]'::jsonb,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table certificate_matches add column if not exists certificate_id uuid;
alter table certificate_matches add column if not exists status text;
alter table certificate_matches add column if not exists match_method text;
alter table certificate_matches add column if not exists cert_volume_m3 numeric(18,6);
alter table certificate_matches add column if not exists allocated_volume_m3 numeric(18,6);
alter table certificate_matches add column if not exists variance_m3 numeric(18,6);
alter table certificate_matches add column if not exists review_note text;
alter table certificate_matches add column if not exists reviewed_by text;
alter table certificate_matches add column if not exists reviewed_at timestamptz;
alter table certificate_matches add column if not exists candidate_sets jsonb default '[]'::jsonb;
alter table certificate_matches add column if not exists diagnostics jsonb default '{}'::jsonb;
alter table certificate_matches add column if not exists created_at timestamptz default now();
alter table certificate_matches add column if not exists updated_at timestamptz default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_matches_certificate_id_fkey'
  ) then
    alter table certificate_matches
      add constraint certificate_matches_certificate_id_fkey
      foreign key (certificate_id) references certificates(id) on delete cascade;
  end if;
end $$;

create unique index if not exists certificate_matches_certificate_id_idx
  on certificate_matches(certificate_id);

create table if not exists certificate_invoice_links (
  id uuid primary key default gen_random_uuid(),
  certificate_match_id uuid not null references certificate_matches(id) on delete cascade,
  certificate_id uuid not null references certificates(id) on delete cascade,
  invoice_row_id uuid not null references invoice_rows(id) on delete restrict,
  row_number integer,
  invoice_no text,
  customer text,
  uplift_date date,
  iata text,
  icao text,
  allocated_m3 numeric(18,6),
  created_at timestamptz default now()
);

alter table certificate_invoice_links add column if not exists certificate_match_id uuid;
alter table certificate_invoice_links add column if not exists certificate_id uuid;
alter table certificate_invoice_links add column if not exists invoice_row_id uuid;
alter table certificate_invoice_links add column if not exists row_number integer;
alter table certificate_invoice_links add column if not exists invoice_no text;
alter table certificate_invoice_links add column if not exists customer text;
alter table certificate_invoice_links add column if not exists uplift_date date;
alter table certificate_invoice_links add column if not exists iata text;
alter table certificate_invoice_links add column if not exists icao text;
alter table certificate_invoice_links add column if not exists allocated_m3 numeric(18,6);
alter table certificate_invoice_links add column if not exists allocation_unit_id uuid;
alter table certificate_invoice_links add column if not exists allocation_unit_index integer;
alter table certificate_invoice_links add column if not exists allocation_unit_type text;
alter table certificate_invoice_links add column if not exists created_at timestamptz default now();

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_certificate_match_id_fkey'
  ) then
    alter table certificate_invoice_links
      add constraint certificate_invoice_links_certificate_match_id_fkey
      foreign key (certificate_match_id) references certificate_matches(id) on delete cascade;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_certificate_id_fkey'
  ) then
    alter table certificate_invoice_links
      add constraint certificate_invoice_links_certificate_id_fkey
      foreign key (certificate_id) references certificates(id) on delete cascade;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_invoice_row_id_fkey'
  ) then
    alter table certificate_invoice_links
      add constraint certificate_invoice_links_invoice_row_id_fkey
      foreign key (invoice_row_id) references invoice_rows(id) on delete restrict;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_allocation_unit_id_fkey'
  ) then
    alter table certificate_invoice_links
      add constraint certificate_invoice_links_allocation_unit_id_fkey
      foreign key (allocation_unit_id) references certificate_allocation_units(id) on delete set null;
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_invoice_row_id_key'
  ) then
    alter table certificate_invoice_links
      drop constraint certificate_invoice_links_invoice_row_id_key;
  end if;
end $$;

drop index if exists certificate_invoice_links_invoice_row_id_idx;
create index if not exists certificate_invoice_links_invoice_row_id_idx
  on certificate_invoice_links(invoice_row_id);
create index if not exists certificate_invoice_links_certificate_idx on certificate_invoice_links(certificate_id);
create index if not exists certificate_invoice_links_match_idx on certificate_invoice_links(certificate_match_id);
create index if not exists certificate_invoice_links_allocation_unit_idx on certificate_invoice_links(allocation_unit_id);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'certificate_invoice_links_allocated_m3_positive_chk'
  ) then
    alter table certificate_invoice_links
      add constraint certificate_invoice_links_allocated_m3_positive_chk
      check (allocated_m3 is not null and allocated_m3 > 0);
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from certificate_invoice_links
    group by certificate_id, invoice_row_id
    having count(*) > 1
  ) then
    raise exception 'Duplicate certificate_invoice_links rows exist for the same certificate_id/invoice_row_id pair. Clean them before applying the unique index.';
  end if;
end $$;

create unique index if not exists certificate_invoice_links_certificate_row_idx
  on certificate_invoice_links(certificate_id, invoice_row_id);

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
  from certificates
  where id = new.certificate_id
  for update;

  if not found then
    raise exception 'Certificate % does not exist', new.certificate_id;
  end if;

  perform 1
  from invoice_rows
  where id = new.invoice_row_id
  for update;

  if not found then
    raise exception 'Invoice row % does not exist', new.invoice_row_id;
  end if;

  select coalesce(saf_vol_m3, 0)
  into v_invoice_capacity
  from invoice_rows
  where id = new.invoice_row_id;

  select coalesce(sum(allocated_m3), 0)
  into v_invoice_allocated
  from certificate_invoice_links
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
  from certificates
  where id = new.certificate_id;

  if v_certificate_capacity is null or v_certificate_capacity <= 0 then
    raise exception 'Certificate % has no valid SAF quantity', new.certificate_id;
  end if;

  select coalesce(sum(allocated_m3), 0)
  into v_certificate_allocated
  from certificate_invoice_links
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

drop trigger if exists trg_enforce_safe_certificate_link_allocation on certificate_invoice_links;
create trigger trg_enforce_safe_certificate_link_allocation
before insert or update of certificate_id, invoice_row_id, allocated_m3
on certificate_invoice_links
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
  from certificates
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

  delete from certificate_matches
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

    insert into certificate_matches (
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
  from invoice_imports
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
      from invoice_rows ir
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
      from certificate_invoice_links
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

  insert into certificate_matches (
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
    insert into certificate_invoice_links (
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

-- Enable RLS — only authenticated users can access data
alter table certificates enable row level security;
alter table invoices enable row level security;
alter table invoice_imports enable row level security;
alter table invoice_rows enable row level security;
alter table certificate_matches enable row level security;
alter table certificate_invoice_links enable row level security;
alter table certificate_allocation_units enable row level security;
alter table client_certificates enable row level security;

drop policy if exists "auth_select_certificates" on certificates;
drop policy if exists "auth_insert_certificates" on certificates;
drop policy if exists "auth_update_certificates" on certificates;
drop policy if exists "auth_delete_certificates" on certificates;

drop policy if exists "auth_select_invoices" on invoices;
drop policy if exists "auth_insert_invoices" on invoices;
drop policy if exists "auth_update_invoices" on invoices;
drop policy if exists "auth_delete_invoices" on invoices;

drop policy if exists "auth_select_invoice_imports" on invoice_imports;
drop policy if exists "auth_insert_invoice_imports" on invoice_imports;
drop policy if exists "auth_update_invoice_imports" on invoice_imports;
drop policy if exists "auth_delete_invoice_imports" on invoice_imports;

drop policy if exists "auth_select_invoice_rows" on invoice_rows;
drop policy if exists "auth_insert_invoice_rows" on invoice_rows;
drop policy if exists "auth_update_invoice_rows" on invoice_rows;
drop policy if exists "auth_delete_invoice_rows" on invoice_rows;

drop policy if exists "auth_select_certificate_matches" on certificate_matches;
drop policy if exists "auth_insert_certificate_matches" on certificate_matches;
drop policy if exists "auth_update_certificate_matches" on certificate_matches;
drop policy if exists "auth_delete_certificate_matches" on certificate_matches;

drop policy if exists "auth_select_certificate_invoice_links" on certificate_invoice_links;
drop policy if exists "auth_insert_certificate_invoice_links" on certificate_invoice_links;
drop policy if exists "auth_update_certificate_invoice_links" on certificate_invoice_links;
drop policy if exists "auth_delete_certificate_invoice_links" on certificate_invoice_links;
drop policy if exists "auth_select_certificate_allocation_units" on certificate_allocation_units;
drop policy if exists "auth_insert_certificate_allocation_units" on certificate_allocation_units;
drop policy if exists "auth_update_certificate_allocation_units" on certificate_allocation_units;
drop policy if exists "auth_delete_certificate_allocation_units" on certificate_allocation_units;
drop policy if exists "auth_select_client_certificates" on client_certificates;
drop policy if exists "auth_insert_client_certificates" on client_certificates;
drop policy if exists "auth_update_client_certificates" on client_certificates;
drop policy if exists "auth_delete_client_certificates" on client_certificates;

create policy "auth_select_certificates" on certificates for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_certificates" on certificates for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_certificates" on certificates for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_certificates" on certificates for delete to authenticated using (auth.uid() is not null);

create policy "auth_select_invoices" on invoices for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_invoices" on invoices for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_invoices" on invoices for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_invoices" on invoices for delete to authenticated using (auth.uid() is not null);

create policy "auth_select_invoice_imports" on invoice_imports for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_invoice_imports" on invoice_imports for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_invoice_imports" on invoice_imports for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_invoice_imports" on invoice_imports for delete to authenticated using (auth.uid() is not null);

create policy "auth_select_invoice_rows" on invoice_rows for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_invoice_rows" on invoice_rows for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_invoice_rows" on invoice_rows for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_invoice_rows" on invoice_rows for delete to authenticated using (auth.uid() is not null);

create policy "auth_select_certificate_matches" on certificate_matches for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_certificate_matches" on certificate_matches for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_certificate_matches" on certificate_matches for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_certificate_matches" on certificate_matches for delete to authenticated using (auth.uid() is not null);

create policy "auth_select_certificate_invoice_links" on certificate_invoice_links for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_certificate_invoice_links" on certificate_invoice_links for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_certificate_invoice_links" on certificate_invoice_links for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_certificate_invoice_links" on certificate_invoice_links for delete to authenticated using (auth.uid() is not null);
create policy "auth_select_certificate_allocation_units" on certificate_allocation_units for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_certificate_allocation_units" on certificate_allocation_units for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_certificate_allocation_units" on certificate_allocation_units for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_certificate_allocation_units" on certificate_allocation_units for delete to authenticated using (auth.uid() is not null);

create policy "auth_select_client_certificates" on client_certificates for select to authenticated using (auth.uid() is not null);
create policy "auth_insert_client_certificates" on client_certificates for insert to authenticated with check (auth.uid() is not null);
create policy "auth_update_client_certificates" on client_certificates for update to authenticated using (auth.uid() is not null) with check (auth.uid() is not null);
create policy "auth_delete_client_certificates" on client_certificates for delete to authenticated using (auth.uid() is not null);

-- Lock helper function lookup to a stable schema search path when these
-- functions already exist in the target database.
do $$
declare
  fn_name text;
  fn_record record;
begin
  foreach fn_name in array array[
    'set_updated_at',
    'try_numeric',
    'is_internal_user',
    'can_read_review_data',
    'can_write_ingestion_data',
    'can_review_and_decide',
    'update_updated_at'
  ]
  loop
    for fn_record in
      select
        n.nspname as schema_name,
        p.proname as function_name,
        pg_get_function_identity_arguments(p.oid) as identity_args
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
      where n.nspname = 'public'
        and p.proname = fn_name
    loop
      execute format(
        'alter function %I.%I(%s) set search_path = public, pg_temp',
        fn_record.schema_name,
        fn_record.function_name,
        fn_record.identity_args
      );
    end loop;
  end loop;
end $$;

-- ── Migration (run if tables already exist) ──────────────────────────────────
-- alter table certificates add column if not exists unique_number text;
-- alter table certificates add column if not exists pdf_path text;
-- alter table invoices add column if not exists csv_path text;
-- alter table invoices drop column if exists rows;
-- create unique index if not exists certificates_unique_number_idx on certificates (unique_number) where unique_number is not null;
-- create table invoice_imports (...);
-- create table invoice_rows (...);
-- create table certificate_matches (...);
-- create table certificate_invoice_links (...);

-- ── Storage bucket: SAF certificate PDFs ────────────────────────────────────
insert into storage.buckets (id, name, public, allowed_mime_types)
values ('certificates-pdf', 'certificates-pdf', false, array['application/pdf'])
on conflict (id) do update set public = false;

drop policy if exists "auth_insert_certificates_pdf" on storage.objects;
drop policy if exists "auth_select_certificates_pdf" on storage.objects;
drop policy if exists "auth_update_certificates_pdf" on storage.objects;
drop policy if exists "auth_delete_certificates_pdf" on storage.objects;

create policy "auth_insert_certificates_pdf" on storage.objects
  for insert to authenticated with check (bucket_id = 'certificates-pdf');

create policy "auth_select_certificates_pdf" on storage.objects
  for select to authenticated using (bucket_id = 'certificates-pdf');

create policy "auth_update_certificates_pdf" on storage.objects
  for update to authenticated using (bucket_id = 'certificates-pdf');

create policy "auth_delete_certificates_pdf" on storage.objects
  for delete to authenticated using (bucket_id = 'certificates-pdf');

-- ── Storage bucket: Invoice CSVs ─────────────────────────────────────────────
insert into storage.buckets (id, name, public, allowed_mime_types)
values ('invoices-csv', 'invoices-csv', false, array['text/csv', 'text/plain'])
on conflict (id) do update set public = false;

drop policy if exists "auth_insert_invoices_csv" on storage.objects;
drop policy if exists "auth_select_invoices_csv" on storage.objects;
drop policy if exists "auth_update_invoices_csv" on storage.objects;
drop policy if exists "auth_delete_invoices_csv" on storage.objects;

create policy "auth_insert_invoices_csv" on storage.objects
  for insert to authenticated with check (bucket_id = 'invoices-csv');

create policy "auth_select_invoices_csv" on storage.objects
  for select to authenticated using (bucket_id = 'invoices-csv');

create policy "auth_update_invoices_csv" on storage.objects
  for update to authenticated using (bucket_id = 'invoices-csv');

create policy "auth_delete_invoices_csv" on storage.objects
  for delete to authenticated using (bucket_id = 'invoices-csv');

-- ── Storage bucket: Generated client certificate PDFs ───────────────────────
insert into storage.buckets (id, name, public, allowed_mime_types)
values ('client-certificates-pdf', 'client-certificates-pdf', false, array['application/pdf'])
on conflict (id) do update set public = false;

drop policy if exists "auth_insert_client_certificates_pdf" on storage.objects;
drop policy if exists "auth_select_client_certificates_pdf" on storage.objects;
drop policy if exists "auth_update_client_certificates_pdf" on storage.objects;
drop policy if exists "auth_delete_client_certificates_pdf" on storage.objects;

create policy "auth_insert_client_certificates_pdf" on storage.objects
  for insert to authenticated with check (bucket_id = 'client-certificates-pdf');

create policy "auth_select_client_certificates_pdf" on storage.objects
  for select to authenticated using (bucket_id = 'client-certificates-pdf');

create policy "auth_update_client_certificates_pdf" on storage.objects
  for update to authenticated using (bucket_id = 'client-certificates-pdf');

create policy "auth_delete_client_certificates_pdf" on storage.objects
  for delete to authenticated using (bucket_id = 'client-certificates-pdf');
