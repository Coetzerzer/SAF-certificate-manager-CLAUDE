create table if not exists public.client_certificates (
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

alter table public.client_certificates add column if not exists group_key text;
alter table public.client_certificates add column if not exists client_name text;
alter table public.client_certificates add column if not exists airport_code text;
alter table public.client_certificates add column if not exists month text;
alter table public.client_certificates add column if not exists total_saf_volume_m3 numeric(18,6) default 0;
alter table public.client_certificates add column if not exists source_certificate_refs jsonb default '[]'::jsonb;
alter table public.client_certificates add column if not exists source_certificate_ids jsonb default '[]'::jsonb;
alter table public.client_certificates add column if not exists source_invoice_row_ids jsonb default '[]'::jsonb;
alter table public.client_certificates add column if not exists source_link_ids jsonb default '[]'::jsonb;
alter table public.client_certificates add column if not exists approved_link_count integer default 0;
alter table public.client_certificates add column if not exists matched_row_count integer default 0;
alter table public.client_certificates add column if not exists issue_date date;
alter table public.client_certificates add column if not exists internal_reference text;
alter table public.client_certificates add column if not exists generated_file_path text;
alter table public.client_certificates add column if not exists created_at timestamptz default now();
alter table public.client_certificates add column if not exists updated_at timestamptz default now();

create unique index if not exists client_certificates_group_key_idx on public.client_certificates(group_key);
create unique index if not exists client_certificates_internal_reference_idx on public.client_certificates(internal_reference);
create index if not exists client_certificates_client_month_idx on public.client_certificates(client_name, airport_code, month);

alter table public.client_certificates enable row level security;

drop policy if exists "auth_select_client_certificates" on public.client_certificates;
drop policy if exists "auth_insert_client_certificates" on public.client_certificates;
drop policy if exists "auth_update_client_certificates" on public.client_certificates;
drop policy if exists "auth_delete_client_certificates" on public.client_certificates;

create policy "auth_select_client_certificates" on public.client_certificates
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_client_certificates" on public.client_certificates
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_client_certificates" on public.client_certificates
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_client_certificates" on public.client_certificates
  for delete to authenticated
  using (auth.uid() is not null);

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
