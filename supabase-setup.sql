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

-- Disable RLS so the publishable key can read/write freely
alter table certificates disable row level security;
alter table invoices disable row level security;

-- ── Migration (run if tables already exist) ──────────────────────────────────
-- alter table certificates add column if not exists unique_number text;
-- alter table certificates add column if not exists pdf_path text;
-- alter table invoices add column if not exists csv_path text;
-- alter table invoices drop column if exists rows;
-- create unique index if not exists certificates_unique_number_idx on certificates (unique_number) where unique_number is not null;

-- ── Storage bucket: SAF certificate PDFs ────────────────────────────────────
insert into storage.buckets (id, name, public, allowed_mime_types)
values ('certificates-pdf', 'certificates-pdf', true, array['application/pdf'])
on conflict (id) do update set public = true;

create policy "anon_insert_certificates_pdf" on storage.objects
  for insert to anon with check (bucket_id = 'certificates-pdf');

create policy "anon_select_certificates_pdf" on storage.objects
  for select to anon using (bucket_id = 'certificates-pdf');

create policy "anon_update_certificates_pdf" on storage.objects
  for update to anon using (bucket_id = 'certificates-pdf');

-- ── Storage bucket: Invoice CSVs ─────────────────────────────────────────────
insert into storage.buckets (id, name, public, allowed_mime_types)
values ('invoices-csv', 'invoices-csv', true, array['text/csv', 'text/plain'])
on conflict (id) do update set public = true;

create policy "anon_insert_invoices_csv" on storage.objects
  for insert to anon with check (bucket_id = 'invoices-csv');

create policy "anon_select_invoices_csv" on storage.objects
  for select to anon using (bucket_id = 'invoices-csv');

create policy "anon_update_invoices_csv" on storage.objects
  for update to anon using (bucket_id = 'invoices-csv');
