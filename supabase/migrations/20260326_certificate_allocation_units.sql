create table if not exists public.certificate_allocation_units (
  id uuid primary key default gen_random_uuid(),
  certificate_id uuid not null references public.certificates(id) on delete cascade,
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

alter table public.certificate_allocation_units
  add column if not exists certificate_id uuid,
  add column if not exists unit_index integer,
  add column if not exists unit_type text,
  add column if not exists airport_iata text,
  add column if not exists airport_icao text,
  add column if not exists airport_name text,
  add column if not exists period_start date,
  add column if not exists period_end date,
  add column if not exists dispatch_date date,
  add column if not exists saf_volume_m3 numeric(18,6),
  add column if not exists jet_volume_m3 numeric(18,6),
  add column if not exists source_reference text,
  add column if not exists matching_mode_override text,
  add column if not exists review_required boolean default false,
  add column if not exists normalization_warning text,
  add column if not exists consumed_volume_m3 numeric(18,6) default 0,
  add column if not exists remaining_volume_m3 numeric(18,6),
  add column if not exists created_at timestamptz default now(),
  add column if not exists updated_at timestamptz default now();

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'certificate_allocation_units_certificate_id_fkey'
  ) then
    alter table public.certificate_allocation_units
      add constraint certificate_allocation_units_certificate_id_fkey
      foreign key (certificate_id) references public.certificates(id) on delete cascade;
  end if;
end $$;

create unique index if not exists certificate_allocation_units_certificate_unit_idx
  on public.certificate_allocation_units(certificate_id, unit_index);
create index if not exists certificate_allocation_units_certificate_idx
  on public.certificate_allocation_units(certificate_id);
create index if not exists certificate_allocation_units_review_idx
  on public.certificate_allocation_units(review_required);

alter table public.certificate_invoice_links
  add column if not exists allocation_unit_id uuid,
  add column if not exists allocation_unit_index integer,
  add column if not exists allocation_unit_type text;

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'certificate_invoice_links_allocation_unit_id_fkey'
  ) then
    alter table public.certificate_invoice_links
      add constraint certificate_invoice_links_allocation_unit_id_fkey
      foreign key (allocation_unit_id) references public.certificate_allocation_units(id) on delete set null;
  end if;
end $$;

create index if not exists certificate_invoice_links_allocation_unit_idx
  on public.certificate_invoice_links(allocation_unit_id);

alter table public.certificate_allocation_units enable row level security;

drop policy if exists "auth_select_certificate_allocation_units" on public.certificate_allocation_units;
drop policy if exists "auth_insert_certificate_allocation_units" on public.certificate_allocation_units;
drop policy if exists "auth_update_certificate_allocation_units" on public.certificate_allocation_units;
drop policy if exists "auth_delete_certificate_allocation_units" on public.certificate_allocation_units;

create policy "auth_select_certificate_allocation_units" on public.certificate_allocation_units
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_certificate_allocation_units" on public.certificate_allocation_units
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_certificate_allocation_units" on public.certificate_allocation_units
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_certificate_allocation_units" on public.certificate_allocation_units
  for delete to authenticated
  using (auth.uid() is not null);
