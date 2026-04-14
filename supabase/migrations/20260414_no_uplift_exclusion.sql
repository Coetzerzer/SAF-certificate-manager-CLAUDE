-- Exclusion flag on allocation units where the covered airport/month has no Titan uplift in the
-- active invoice import. These units cannot be matched (no invoices exist), so they should be
-- excluded from the coverage ratio denominator instead of penalising it.
--
-- Applied live to booddnrpwvphgurfixll on 2026-04-14.
--
-- Use case: TotalEnergies PoC France lists 38 airports; only 13 had Titan uplifts in 2025.
-- The other 25 regional airports have zero invoices and must not drag coverage % down.

ALTER TABLE public.certificate_allocation_units
  ADD COLUMN IF NOT EXISTS excluded_no_uplift boolean NOT NULL DEFAULT false;

-- Recompute excluded_no_uplift for all units based on the currently active invoice import.
-- A unit is excluded if and only if:
--   - It has a valid airport_iata and period_start
--   - Zero invoice rows exist for that (airport, month) with saf_vol_m3 > 0
CREATE OR REPLACE FUNCTION public.sync_no_uplift_exclusions()
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
  v_count integer;
BEGIN
  WITH pools AS (
    SELECT UPPER(ir.iata) AS airport, TO_CHAR(ir.uplift_date, 'YYYY-MM') AS month
    FROM public.invoice_rows ir
    JOIN public.invoice_imports ii ON ii.id = ir.import_id AND ii.status = 'active'
    WHERE ir.saf_vol_m3::numeric > 0
    GROUP BY 1, 2
  ),
  computed AS (
    SELECT
      au.id,
      (au.airport_iata IS NOT NULL
        AND au.airport_iata <> ''
        AND au.period_start IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM pools p
          WHERE p.airport = UPPER(au.airport_iata)
            AND p.month = TO_CHAR(au.period_start, 'YYYY-MM')
        )) AS should_exclude
    FROM public.certificate_allocation_units au
  ),
  changed AS (
    UPDATE public.certificate_allocation_units au
    SET excluded_no_uplift = c.should_exclude,
        updated_at = now()
    FROM computed c
    WHERE au.id = c.id
      AND au.excluded_no_uplift IS DISTINCT FROM c.should_exclude
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_count FROM changed;
  RETURN COALESCE(v_count, 0);
END
$func$;

GRANT EXECUTE ON FUNCTION public.sync_no_uplift_exclusions() TO anon, authenticated, service_role;
