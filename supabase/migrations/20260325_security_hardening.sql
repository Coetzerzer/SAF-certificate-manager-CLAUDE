-- Harden RLS policies flagged by the Supabase linter.
drop policy if exists "auth_select_certificates" on public.certificates;
drop policy if exists "auth_insert_certificates" on public.certificates;
drop policy if exists "auth_update_certificates" on public.certificates;
drop policy if exists "auth_delete_certificates" on public.certificates;

drop policy if exists "auth_select_invoices" on public.invoices;
drop policy if exists "auth_insert_invoices" on public.invoices;
drop policy if exists "auth_update_invoices" on public.invoices;
drop policy if exists "auth_delete_invoices" on public.invoices;

drop policy if exists "auth_select_invoice_imports" on public.invoice_imports;
drop policy if exists "auth_insert_invoice_imports" on public.invoice_imports;
drop policy if exists "auth_update_invoice_imports" on public.invoice_imports;
drop policy if exists "auth_delete_invoice_imports" on public.invoice_imports;

drop policy if exists "auth_select_invoice_rows" on public.invoice_rows;
drop policy if exists "auth_insert_invoice_rows" on public.invoice_rows;
drop policy if exists "auth_update_invoice_rows" on public.invoice_rows;
drop policy if exists "auth_delete_invoice_rows" on public.invoice_rows;

drop policy if exists "auth_select_certificate_matches" on public.certificate_matches;
drop policy if exists "auth_insert_certificate_matches" on public.certificate_matches;
drop policy if exists "auth_update_certificate_matches" on public.certificate_matches;
drop policy if exists "auth_delete_certificate_matches" on public.certificate_matches;

drop policy if exists "auth_select_certificate_invoice_links" on public.certificate_invoice_links;
drop policy if exists "auth_insert_certificate_invoice_links" on public.certificate_invoice_links;
drop policy if exists "auth_update_certificate_invoice_links" on public.certificate_invoice_links;
drop policy if exists "auth_delete_certificate_invoice_links" on public.certificate_invoice_links;

create policy "auth_select_certificates" on public.certificates
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_certificates" on public.certificates
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_certificates" on public.certificates
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_certificates" on public.certificates
  for delete to authenticated
  using (auth.uid() is not null);

create policy "auth_select_invoices" on public.invoices
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_invoices" on public.invoices
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_invoices" on public.invoices
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_invoices" on public.invoices
  for delete to authenticated
  using (auth.uid() is not null);

create policy "auth_select_invoice_imports" on public.invoice_imports
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_invoice_imports" on public.invoice_imports
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_invoice_imports" on public.invoice_imports
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_invoice_imports" on public.invoice_imports
  for delete to authenticated
  using (auth.uid() is not null);

create policy "auth_select_invoice_rows" on public.invoice_rows
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_invoice_rows" on public.invoice_rows
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_invoice_rows" on public.invoice_rows
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_invoice_rows" on public.invoice_rows
  for delete to authenticated
  using (auth.uid() is not null);

create policy "auth_select_certificate_matches" on public.certificate_matches
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_certificate_matches" on public.certificate_matches
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_certificate_matches" on public.certificate_matches
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_certificate_matches" on public.certificate_matches
  for delete to authenticated
  using (auth.uid() is not null);

create policy "auth_select_certificate_invoice_links" on public.certificate_invoice_links
  for select to authenticated
  using (auth.uid() is not null);

create policy "auth_insert_certificate_invoice_links" on public.certificate_invoice_links
  for insert to authenticated
  with check (auth.uid() is not null);

create policy "auth_update_certificate_invoice_links" on public.certificate_invoice_links
  for update to authenticated
  using (auth.uid() is not null)
  with check (auth.uid() is not null);

create policy "auth_delete_certificate_invoice_links" on public.certificate_invoice_links
  for delete to authenticated
  using (auth.uid() is not null);

-- Fix the mutable search_path warning on helper functions if they already exist.
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
