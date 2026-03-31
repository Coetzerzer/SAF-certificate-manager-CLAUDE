alter table public.invoice_imports
  alter column status set default 'staging';

alter table public.invoice_imports
  add column if not exists candidate_row_count integer not null default 0;

alter table public.invoice_imports
  add column if not exists invalid_row_count integer not null default 0;

alter table public.invoice_imports
  add column if not exists duplicate_row_count integer not null default 0;

alter table public.invoice_imports
  add column if not exists validation_summary jsonb not null default '{}'::jsonb;

alter table public.invoice_imports
  add column if not exists activated_at timestamptz;

alter table public.invoice_imports
  add column if not exists failed_at timestamptz;

alter table public.invoice_rows
  add column if not exists is_duplicate boolean not null default false;

alter table public.invoice_rows
  add column if not exists duplicate_group_key text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'invoice_imports_status_valid_chk'
  ) then
    alter table public.invoice_imports
      add constraint invoice_imports_status_valid_chk
      check (status in ('staging', 'active', 'failed', 'superseded'));
  end if;
end $$;

create index if not exists invoice_imports_status_idx
  on public.invoice_imports(status, year, created_at desc);

create index if not exists invoice_rows_import_duplicate_idx
  on public.invoice_rows(import_id, is_duplicate, duplicate_group_key);

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

  -- Only delete matches for certificates whose coverage year matches this import's year.
  -- This preserves approved matches for other years when re-importing a single year's CSV.
  delete from public.certificate_matches
  where certificate_id in (
    select c.id
    from public.certificates c
    where extract(year from to_date(
      nullif(coalesce(c.data->>'coverageMonth', ''), '') || '-01',
      'YYYY-MM-DD'
    )) = v_target.year
  )
  or certificate_id in (
    -- Also clear matches for certificates with no coverage month (they need re-evaluation)
    select c.id
    from public.certificates c
    where coalesce(c.data->>'coverageMonth', '') = ''
  );

  -- Only reset allocation units for the same scoped certificates.
  update public.certificate_allocation_units
  set consumed_volume_m3 = 0,
      remaining_volume_m3 = case
        when saf_volume_m3 is null then null
        else greatest(0, saf_volume_m3)
      end,
      updated_at = now()
  where certificate_id in (
    select c.id
    from public.certificates c
    where coalesce(c.data->>'coverageMonth', '') = ''
       or extract(year from to_date(
            nullif(coalesce(c.data->>'coverageMonth', ''), '') || '-01',
            'YYYY-MM-DD'
          )) = v_target.year
  );

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
