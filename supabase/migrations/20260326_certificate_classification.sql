alter table public.certificates
  add column if not exists document_family text,
  add column if not exists matching_mode text,
  add column if not exists classification_confidence numeric(5,4),
  add column if not exists review_required boolean default false,
  add column if not exists classification_reason text;

update public.certificates
set
  document_family = coalesce(document_family, nullif(data->>'document_family', '')),
  matching_mode = coalesce(matching_mode, nullif(data->>'matching_mode', ''), nullif(data->>'matchingMode', '')),
  classification_confidence = coalesce(
    classification_confidence,
    nullif(data->>'classification_confidence', '')::numeric,
    nullif(data->>'classificationConfidence', '')::numeric
  ),
  review_required = coalesce(
    review_required,
    case
      when data ? 'review_required' then coalesce((data->>'review_required')::boolean, false)
      when data ? 'reviewRequired' then coalesce((data->>'reviewRequired')::boolean, false)
      else false
    end
  ),
  classification_reason = coalesce(
    classification_reason,
    nullif(data->>'classification_reason', ''),
    nullif(data->>'classificationReason', '')
  )
where
  document_family is null
  or matching_mode is null
  or classification_confidence is null
  or review_required is null
  or classification_reason is null;

create index if not exists certificates_matching_mode_idx on public.certificates (matching_mode);
create index if not exists certificates_review_required_idx on public.certificates (review_required);
