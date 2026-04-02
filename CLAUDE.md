# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run dev       # Start dev server at http://localhost:5173
npm run build     # Production build
npm run preview   # Preview production build
```

No test runner is configured.

## Environment

The frontend does not need an AI API key anymore.

Set the Anthropic key as a Supabase Edge Function secret:

```
ANTHROPIC_API_KEY=sk-ant-...
```

Optional:

```
ANTHROPIC_MODEL=claude-sonnet-4-20250514
```

The Supabase client is hardcoded in `src/supabase.js` (URL + publishable key). No env variable needed in the frontend for Supabase.

## Architecture

This is a single-file React application (`SAF_Certificate_Manager.jsx`) with no routing, no component library, and no state management library. All logic lives in one file.

**Entry point:** `src/main.jsx` → renders `<SAFManager />` from `SAF_Certificate_Manager.jsx`.

### Data flow

1. **PDF upload** → Supabase Edge Function (`extract-certificate`) → Anthropic Messages API (Claude Sonnet) → structured JSON cert data → Supabase `certificates` table + `certificates-pdf` storage bucket
2. **CSV upload** → staged import pipeline: file to `invoices-csv` bucket → metadata row in `invoice_imports` (status=staging) → rows inserted into `invoice_rows` → `activate_invoice_import()` RPC supersedes previous active import for the same year, clears year-scoped matches, activates new import
3. **On mount** (`loadFromDB`): loads all certs from `certificates` table + latest active invoice import + invoice rows from `invoice_rows` + matches/links/allocation units. Falls back to legacy `invoices` table if no `invoice_imports` exist.
4. **Matching** → DB-side FIFO allocation via `allocate_simple_certificate` stored procedure → filters invoice rows by airport code + coverage month → allocates by uplift date → saved into `certificate_matches` + `certificate_invoice_links`

### Certificate types

- **PoS** (Proof of Sustainability): single shipment cert
- **PoC** (Proof of Compliance): umbrella cert covering multiple airports over a supply period
- **Complex PoC**: PoC with 3+ airports or `isComplexPoC === "true"`. Currently classified as `manual_only` — no automated matching is performed for complex PoCs. They require manual review and allocation outside this tool.

### Key functions in `SAFManager` component

| Function | Purpose |
|---|---|
| `loadFromDB` | Loads certs + latest CSV on mount |
| `handlePDFUpload` | Extracts cert from PDF via Supabase Edge Function + Anthropic Claude, upserts to DB |
| `handleCSVUpload` | Uploads CSV to storage, inserts staged import row, activates import |
| `analyzeAll` / `analyzeSingle` | Runs DB-side FIFO allocation via `allocate_simple_certificate` stored procedure |
| `reExtractCert` | Downloads stored PDF from bucket, re-runs extraction via Edge Function |
| `runDatabaseSimpleAllocation` | Calls the `allocate_simple_certificate` RPC for a single certificate |

### Supabase schema

**Tables:**
- `certificates`: `id, filename, unique_number (unique), data (jsonb), analysis (jsonb), pdf_path, document_family, matching_mode, classification_confidence, review_required, classification_reason, created_at`
- `invoice_imports`: `id, filename, storage_path, year, status (staging|active|failed|superseded), row_count, candidate_row_count, invalid_row_count, duplicate_row_count, validation_summary (jsonb), activated_at, failed_at, created_at`
- `invoice_rows`: `id, import_id, row_number, invoice_no, customer, uplift_date, flight_no, delivery_ticket, iata, icao, country, supplier, vol_m3, saf_vol_m3, is_allocated, is_duplicate, duplicate_group_key, validation_note, raw_payload (jsonb)`
- `certificate_matches`: `id, certificate_id, status, match_method, cert_volume_m3, allocated_volume_m3, variance_m3, review_note, reviewed_by, reviewed_at, candidate_sets (jsonb), diagnostics (jsonb), created_at, updated_at`
- `certificate_invoice_links`: `id, certificate_match_id, certificate_id, invoice_row_id, row_number, invoice_no, customer, uplift_date, iata, icao, allocated_m3, allocation_unit_id, allocation_unit_index, allocation_unit_type`
- `certificate_allocation_units`: `id, certificate_id, unit_index, unit_type, airport_code, month, saf_volume_m3, consumed_volume_m3, remaining_volume_m3`
- `client_certificates`: `id, group_key (unique), client_name, airport_code, month, total_saf_volume_m3, source_certificate_refs, generated_file_path, issue_date, internal_reference`
- `invoices` (legacy): `id, filename, csv_path, created_at`

**Storage buckets:**
- `certificates-pdf`: PDF files keyed by `{uniqueNumber}.pdf` or `no-id/{timestamp}-{filename}`
- `invoices-csv`: CSV files keyed by `invoices/{filename}`
- `client-certificates-pdf`: Generated client certificate PDFs

### Extraction

Certificate extraction prompting now lives in the Supabase Edge Function:
- `supabase/functions/extract-certificate/index.ts`

The frontend calls `supabase.functions.invoke("extract-certificate")` and still uses `normalizeCommaDecimals()` as a safety net on the returned JSON.

### UI structure

Three tabs rendered inline in `SAFManager`:
- **CERTS**: list of `CertCard` components + detail panel (`FieldRow`, `GHGBar` sub-components)
- **ANALYSIS**: compliance results, attribution table for complex PoCs
- **DB**: raw JSON viewer for certs and invoices state

All styling is inline CSS with a dark blue/cyan color scheme. No CSS files or CSS-in-JS library.
