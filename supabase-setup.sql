-- Run this in your Supabase dashboard: https://supabase.com/dashboard/project/booddnrpwvphgurfixll/sql/new

create table if not exists certificates (
  id uuid primary key default gen_random_uuid(),
  filename text not null,
  data jsonb not null default '{}',
  analysis jsonb,
  unique_number text,
  pdf_path text,
  created_at timestamptz default now()
);

-- Deduplicate by SAF unique number (null values are excluded so blanks never conflict)
create unique index if not exists certificates_unique_number_idx
  on certificates (unique_number)
  where unique_number is not null;

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
  status text not null default 'active',
  row_count integer not null default 0,
  created_at timestamptz default now()
);

create unique index if not exists invoice_imports_active_year_idx
  on invoice_imports (year)
  where status = 'active';

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
  raw_payload jsonb not null default '{}'::jsonb,
  is_allocated boolean not null default false,
  created_at timestamptz default now()
);

alter table invoice_imports add column if not exists filename text;
alter table invoice_imports add column if not exists storage_path text;
alter table invoice_imports add column if not exists year integer default 2025;
alter table invoice_imports add column if not exists status text default 'active';
alter table invoice_imports add column if not exists row_count integer default 0;
alter table invoice_imports add column if not exists created_at timestamptz default now();

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
alter table invoice_rows add column if not exists raw_payload jsonb default '{}'::jsonb;
alter table invoice_rows add column if not exists is_allocated boolean default false;
alter table invoice_rows add column if not exists created_at timestamptz default now();

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
  invoice_row_id uuid not null unique references invoice_rows(id) on delete restrict,
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
end $$;

create unique index if not exists certificate_invoice_links_invoice_row_id_idx
  on certificate_invoice_links(invoice_row_id);
create index if not exists certificate_invoice_links_certificate_idx on certificate_invoice_links(certificate_id);
create index if not exists certificate_invoice_links_match_idx on certificate_invoice_links(certificate_match_id);

-- Enable RLS — only authenticated users can access data
alter table certificates enable row level security;
alter table invoices enable row level security;
alter table invoice_imports enable row level security;
alter table invoice_rows enable row level security;
alter table certificate_matches enable row level security;
alter table certificate_invoice_links enable row level security;

create policy "auth_select_certificates" on certificates for select to authenticated using (true);
create policy "auth_insert_certificates" on certificates for insert to authenticated with check (true);
create policy "auth_update_certificates" on certificates for update to authenticated using (true);
create policy "auth_delete_certificates" on certificates for delete to authenticated using (true);

create policy "auth_select_invoices" on invoices for select to authenticated using (true);
create policy "auth_insert_invoices" on invoices for insert to authenticated with check (true);
create policy "auth_update_invoices" on invoices for update to authenticated using (true);
create policy "auth_delete_invoices" on invoices for delete to authenticated using (true);

create policy "auth_select_invoice_imports" on invoice_imports for select to authenticated using (true);
create policy "auth_insert_invoice_imports" on invoice_imports for insert to authenticated with check (true);
create policy "auth_update_invoice_imports" on invoice_imports for update to authenticated using (true);
create policy "auth_delete_invoice_imports" on invoice_imports for delete to authenticated using (true);

create policy "auth_select_invoice_rows" on invoice_rows for select to authenticated using (true);
create policy "auth_insert_invoice_rows" on invoice_rows for insert to authenticated with check (true);
create policy "auth_update_invoice_rows" on invoice_rows for update to authenticated using (true);
create policy "auth_delete_invoice_rows" on invoice_rows for delete to authenticated using (true);

create policy "auth_select_certificate_matches" on certificate_matches for select to authenticated using (true);
create policy "auth_insert_certificate_matches" on certificate_matches for insert to authenticated with check (true);
create policy "auth_update_certificate_matches" on certificate_matches for update to authenticated using (true);
create policy "auth_delete_certificate_matches" on certificate_matches for delete to authenticated using (true);

create policy "auth_select_certificate_invoice_links" on certificate_invoice_links for select to authenticated using (true);
create policy "auth_insert_certificate_invoice_links" on certificate_invoice_links for insert to authenticated with check (true);
create policy "auth_update_certificate_invoice_links" on certificate_invoice_links for update to authenticated using (true);
create policy "auth_delete_certificate_invoice_links" on certificate_invoice_links for delete to authenticated using (true);

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

create policy "auth_insert_invoices_csv" on storage.objects
  for insert to authenticated with check (bucket_id = 'invoices-csv');

create policy "auth_select_invoices_csv" on storage.objects
  for select to authenticated using (bucket_id = 'invoices-csv');

create policy "auth_update_invoices_csv" on storage.objects
  for update to authenticated using (bucket_id = 'invoices-csv');

create policy "auth_delete_invoices_csv" on storage.objects
  for delete to authenticated using (bucket_id = 'invoices-csv');
