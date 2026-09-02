-- =========================================================
-- PostgreSQL Source Initialization
-- =========================================================

CREATE TABLE IF NOT EXISTS public.customers (
    id BIGSERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Automatically update updated_at whenever a row is updated.
CREATE OR REPLACE FUNCTION public.set_customers_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop first so this script remains idempotent when executed again.
DROP TRIGGER IF EXISTS customers_updated_at_trigger
ON public.customers;

CREATE TRIGGER customers_updated_at_trigger
BEFORE UPDATE ON public.customers
FOR EACH ROW
EXECUTE FUNCTION public.set_customers_updated_at();

-- Initial records for testing Debezium initial snapshot.
INSERT INTO public.customers (
    full_name,
    email,
    status
)
VALUES
    ('Reza Mardiansyah', 'reza@example.com', 'active'),
    ('Aulia Rahman', 'aulia@example.com', 'active'),
    ('Siti Aisyah', 'siti@example.com', 'inactive'),
    ('Budi Santoso', 'budi@example.com', 'active'),
    ('Nadia Putri', 'nadia@example.com', 'active')
ON CONFLICT (email) DO NOTHING;
