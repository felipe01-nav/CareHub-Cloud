-- CareHub: runs once, the first time the database is created.
-- Prepares the "auth" schema and roles expected by the open-source sign-in
-- service (supabase/auth). Safe to run more than once.
CREATE SCHEMA IF NOT EXISTS auth;

DO $$
DECLARE
  r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['supabase_auth_admin', 'supabase_admin', 'authenticated', 'anon', 'service_role']
  LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN', r);
    END IF;
  END LOOP;
END
$$;
