-- Reconciliation RPCs for derived flags on invoice_rows and certificate_allocation_units.
-- Applied live to booddnrpwvphgurfixll on 2026-04-14.
--
-- Rationale: certificate_invoice_links is the source of truth for allocation.
-- Other tables carry cached/derived fields that drift over time:
--   - invoice_rows.is_allocated
--   - certificate_allocation_units.consumed_volume_m3 / remaining_volume_m3
-- These two RPCs recompute them idempotently from the link ledger. They are
-- invoked at the end of analyzeAll / analyzeSingle in the frontend as a
-- best-effort post-match reconciliation.

CREATE OR REPLACE FUNCTION public.sync_invoice_allocation_flags()
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
  v_count integer;
BEGIN
  WITH computed AS (
    SELECT
      ir.id,
      ir.is_allocated AS current_flag,
      (COALESCE((SELECT SUM(l.allocated_m3::numeric)
                 FROM public.certificate_invoice_links l
                 WHERE l.invoice_row_id = ir.id), 0)
        + 0.002 >= ir.saf_vol_m3::numeric
       AND ir.saf_vol_m3::numeric > 0) AS should_be
    FROM public.invoice_rows ir
  ),
  changed AS (
    UPDATE public.invoice_rows ir
    SET is_allocated = c.should_be
    FROM computed c
    WHERE ir.id = c.id
      AND ir.is_allocated IS DISTINCT FROM c.should_be
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_count FROM changed;
  RETURN COALESCE(v_count, 0);
END
$func$;

GRANT EXECUTE ON FUNCTION public.sync_invoice_allocation_flags() TO anon, authenticated, service_role;

-- Sync certificate_allocation_units.consumed_volume_m3 from the link ledger.
-- - For supported_poc certs: sum links by allocation_unit_id (units have per-month identity).
-- - For supported_simple certs (and legacy): sum links by certificate_id (1 unit per cert,
--   historical links often have NULL allocation_unit_id).
CREATE OR REPLACE FUNCTION public.sync_allocation_units_consumed()
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
  v_count integer;
BEGIN
  WITH computed AS (
    SELECT
      au.id,
      au.saf_volume_m3::numeric AS saf,
      CASE
        WHEN c.document_family = 'supported_poc' THEN
          COALESCE((SELECT SUM(l.allocated_m3::numeric)
                    FROM public.certificate_invoice_links l
                    WHERE l.allocation_unit_id = au.id), 0)
        ELSE
          COALESCE((SELECT SUM(l.allocated_m3::numeric)
                    FROM public.certificate_invoice_links l
                    WHERE l.certificate_id = c.id), 0)
      END AS new_consumed
    FROM public.certificate_allocation_units au
    JOIN public.certificates c ON c.id = au.certificate_id
  ),
  changed AS (
    UPDATE public.certificate_allocation_units au
    SET consumed_volume_m3 = c.new_consumed,
        remaining_volume_m3 = GREATEST(0, c.saf - c.new_consumed),
        updated_at = now()
    FROM computed c
    WHERE au.id = c.id
      AND ABS(au.consumed_volume_m3::numeric - c.new_consumed) > 0.001
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_count FROM changed;
  RETURN COALESCE(v_count, 0);
END
$func$;

GRANT EXECUTE ON FUNCTION public.sync_allocation_units_consumed() TO anon, authenticated, service_role;
