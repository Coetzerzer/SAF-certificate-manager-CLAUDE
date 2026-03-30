# Certificate Classification Rules

This app classifies each certificate with deterministic business rules before matching.

## Persisted fields

- `document_family`
- `matching_mode`
- `classification_confidence`
- `review_required`
- `classification_reason`

These values are stored on `certificates` and mirrored into the certificate `data` JSON for safe backfill and reviewability.

## Document families

- `simple_monthly_airport_pos`
  Used for PoS certificates with one airport, month-level coverage, and no strong single-uplift wording.
  Matching mode: `monthly-pos`

- `single_uplift_pos`
  Used for PoS certificates that look like one shipment or one uplift event.
  Matching mode: `pos-uplift`

- `monthly_airport_poc`
  Used for PoC certificates with airport/month tables or otherwise clear monthly airport coverage.
  Matching mode: `poc-monthly`

- `annual_consolidated_poc`
  Used for long-span or umbrella PoC certificates, especially full-year or multi-airport aggregate documents.
  Matching mode: `poc-airport`
  Review is usually required because these documents are often operationally ambiguous.

- `hybrid_multi_source`
  Used when a certificate mixes signals from multiple operational shapes and does not fit a standard flow.
  Matching mode: `needs_review`

- `unknown`
  Used when deterministic rules cannot confidently place the certificate.
  Matching mode: `needs_review`

## Main signals used

- document type (`PoS` vs `PoC`)
- number of canonical airports
- number of months covered
- presence of `underlyingPoSList`
- presence of `monthlyVolumes`
- presence of `airportVolumes`
- supply period length
- wording that suggests a single uplift or shipment

## Review rules

`review_required` is set to `true` when:

- critical fields are missing for the chosen family
- the document shape is ambiguous
- the classifier lands on `hybrid_multi_source`
- the classifier lands on `unknown`
- the document is an annual or umbrella-style PoC that still needs human confirmation
