-- Atomic certificate-match writes
--
-- persistMatch (clear + insert match + insert links) and saveManualMatch's
-- append branch (insert links + update match summary) were each non-atomic
-- across multiple Supabase calls. A failure mid-sequence could leave links
-- without a match summary that reflects them, or wipe a previously approved
-- match without anything to restore. This RPC runs the whole replace-or-append
-- inside a single Postgres transaction so the ledger and summary stay in sync.

create or replace function public.upsert_certificate_match(
  p_certificate_id uuid,
  p_match jsonb,
  p_links jsonb,
  p_mode text
) returns uuid
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_match_id uuid;
begin
  if p_certificate_id is null then
    raise exception 'p_certificate_id is required';
  end if;
  if p_mode not in ('replace','append') then
    raise exception 'p_mode must be replace or append, got %', p_mode;
  end if;

  if p_mode = 'replace' then
    delete from public.certificate_matches where certificate_id = p_certificate_id;

    insert into public.certificate_matches (
      certificate_id, status, match_method, cert_volume_m3, allocated_volume_m3,
      variance_m3, review_note, reviewed_by, reviewed_at, candidate_sets, diagnostics, updated_at
    ) values (
      p_certificate_id,
      coalesce(p_match->>'status','pending'),
      p_match->>'match_method',
      nullif(p_match->>'cert_volume_m3','')::numeric,
      nullif(p_match->>'allocated_volume_m3','')::numeric,
      nullif(p_match->>'variance_m3','')::numeric,
      p_match->>'review_note',
      p_match->>'reviewed_by',
      nullif(p_match->>'reviewed_at','')::timestamptz,
      coalesce(p_match->'candidate_sets','[]'::jsonb),
      coalesce(p_match->'diagnostics','{}'::jsonb),
      now()
    )
    returning id into v_match_id;
  else
    select id into v_match_id from public.certificate_matches where certificate_id = p_certificate_id;
    if v_match_id is null then
      raise exception 'append mode requires existing match for certificate %', p_certificate_id;
    end if;
    update public.certificate_matches set
      status = coalesce(p_match->>'status', status),
      match_method = coalesce(p_match->>'match_method', match_method),
      allocated_volume_m3 = coalesce(nullif(p_match->>'allocated_volume_m3','')::numeric, allocated_volume_m3),
      variance_m3 = coalesce(nullif(p_match->>'variance_m3','')::numeric, variance_m3),
      review_note = coalesce(p_match->>'review_note', review_note),
      reviewed_by = coalesce(p_match->>'reviewed_by', reviewed_by),
      reviewed_at = coalesce(nullif(p_match->>'reviewed_at','')::timestamptz, reviewed_at),
      updated_at = now()
    where id = v_match_id;
  end if;

  if p_links is not null and jsonb_typeof(p_links) = 'array' and jsonb_array_length(p_links) > 0 then
    insert into public.certificate_invoice_links (
      certificate_match_id, certificate_id, invoice_row_id, row_number,
      invoice_no, customer, uplift_date, iata, icao, allocated_m3,
      allocation_unit_id, allocation_unit_index, allocation_unit_type
    )
    select
      v_match_id,
      p_certificate_id,
      (l->>'invoice_row_id')::uuid,
      nullif(l->>'row_number','')::int,
      l->>'invoice_no',
      l->>'customer',
      nullif(l->>'uplift_date','')::date,
      l->>'iata',
      l->>'icao',
      (l->>'allocated_m3')::numeric,
      nullif(l->>'allocation_unit_id','')::uuid,
      nullif(l->>'allocation_unit_index','')::int,
      l->>'allocation_unit_type'
    from jsonb_array_elements(p_links) l;
  end if;

  return v_match_id;
end;
$$;

grant execute on function public.upsert_certificate_match(uuid, jsonb, jsonb, text) to authenticated;
