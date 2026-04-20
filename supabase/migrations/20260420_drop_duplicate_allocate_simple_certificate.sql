-- Migration: drop the legacy 3-arg overload of allocate_simple_certificate.
--
-- A 4-arg version (with p_force_reallocate boolean default false) was added to
-- the database out-of-band (not via this migrations folder). Postgres keeps both
-- overloads and cannot disambiguate when the frontend calls with 3 positional
-- args, producing:
--   "Could not choose the best candidate function between: ..."
-- and breaking auto-matching for every certificate already at status=approved
-- (the only path the 4-arg version protects with p_force_reallocate).
--
-- The 4-arg version is a strict superset: same body + an optional flag that
-- allows re-allocating already-approved certs. Dropping the 3-arg overload
-- keeps current frontend behaviour unchanged (default flag = false).

drop function if exists public.allocate_simple_certificate(uuid, uuid, text);
