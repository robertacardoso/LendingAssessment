-- db_tables.sql
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_name = 'clients' OR table_name = 'loans'
)