-- Ensure bucket exists and is private (signed URLs used by frontend).
insert into storage.buckets (id, name, public, allowed_mime_types)
values ('certificates-pdf', 'certificates-pdf', false, array['application/pdf'])
on conflict (id) do update
  set public = excluded.public,
      allowed_mime_types = excluded.allowed_mime_types;

drop policy if exists "anon_select_certificates_pdf" on storage.objects;
drop policy if exists "anon_insert_certificates_pdf" on storage.objects;
drop policy if exists "anon_update_certificates_pdf" on storage.objects;
drop policy if exists "anon_delete_certificates_pdf" on storage.objects;

-- Même modèle que pour les autres buckets : accès via clé publishable (anon).
create policy "anon_select_certificates_pdf" on storage.objects
  for select to anon using (bucket_id = 'certificates-pdf');

create policy "anon_insert_certificates_pdf" on storage.objects
  for insert to anon with check (bucket_id = 'certificates-pdf');

create policy "anon_update_certificates_pdf" on storage.objects
  for update to anon using (bucket_id = 'certificates-pdf');

create policy "anon_delete_certificates_pdf" on storage.objects
  for delete to anon using (bucket_id = 'certificates-pdf');
