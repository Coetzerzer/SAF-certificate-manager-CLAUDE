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

Create a `.env` file at the project root:

```
VITE_ANTHROPIC_KEY=sk-ant-...
```

The Supabase client is hardcoded in `src/supabase.js` (URL + publishable key). No env variable needed for Supabase.

## Architecture

This is a single-file React application (`SAF_Certificate_Manager.jsx`) with no routing, no component library, and no state management library. All logic lives in one file.

**Entry point:** `src/main.jsx` → renders `<SAFManager />` from `SAF_Certificate_Manager.jsx`.

### Data flow

1. **PDF upload** → Claude API (direct browser call, `anthropic-dangerous-direct-browser-access` header) → structured JSON cert data → Supabase `certificates` table + `certificates-pdf` storage bucket
2. **CSV upload** → Supabase `invoices-csv` storage bucket → metadata row in `invoices` table → parsed into local `invoices` state
3. **On mount** (`loadFromDB`): loads all certs from `certificates` table + downloads latest CSV from `invoices-csv` bucket (skips records with null `csv_path`)
4. **Analysis** → Claude API with cert JSON + filtered invoice rows → JSON compliance result → saved back to `certificates.analysis` column

### Certificate types

- **PoS** (Proof of Sustainability): single shipment cert
- **PoC** (Proof of Compliance): umbrella cert covering multiple airports over a supply period
- **Complex PoC**: PoC with 3+ airports; triggers a two-step analysis (`analyzeComplexPoC`) — first compliance check, then client attribution

`certIsComplex()` detects complex PoC by checking `isComplexPoC === "true"` or PoC + ≥3 airports.

### Key functions in `SAFManager` component

| Function | Purpose |
|---|---|
| `loadFromDB` | Loads certs + latest CSV on mount |
| `handlePDFUpload` | Extracts cert from PDF via Claude, upserts to DB |
| `handleCSVUpload` | Uploads CSV to storage, inserts metadata row |
| `analyzeAll` / `analyzeSingle` | Standard compliance analysis via `COMPARE_PROMPT` |
| `analyzeComplexPoC` | Two-step analysis (compliance + attribution) for complex PoCs |
| `reExtractCert` | Downloads stored PDF from bucket, re-runs extraction |
| `filterInvoicesForCert` | Filters invoice rows by airport codes and supply period before sending to Claude |

### Supabase schema

**Tables:**
- `certificates`: `id, filename, unique_number (unique), data (jsonb), analysis (jsonb), pdf_path, created_at`
- `invoices`: `id, filename, csv_path, created_at`

**Storage buckets:**
- `certificates-pdf`: PDF files keyed by `{uniqueNumber}.pdf` or `no-id/{timestamp}-{filename}`
- `invoices-csv`: CSV files keyed by `invoices/{filename}`

### Claude prompts

Three prompts are defined as module-level constants:
- `EXTRACT_PROMPT`: extracts ~35 structured fields from a PDF cert (used at upload and re-extract time)
- `COMPARE_PROMPT(cert, invoices)`: compliance analysis comparing cert fields to invoice rows
- `ATTRIBUTION_PROMPT(cert, invoices)`: customer attribution for complex PoCs — maps cert volume slots (airport × month) to invoice customers

`normalizeCommaDecimals()` post-processes Claude's JSON output to convert European comma-decimal notation (e.g. `"4,2"` → `"4.2"`) on a fixed set of numeric fields.

### UI structure

Three tabs rendered inline in `SAFManager`:
- **CERTS**: list of `CertCard` components + detail panel (`FieldRow`, `GHGBar` sub-components)
- **ANALYSIS**: compliance results, attribution table for complex PoCs
- **DB**: raw JSON viewer for certs and invoices state

All styling is inline CSS with a dark blue/cyan color scheme. No CSS files or CSS-in-JS library.
