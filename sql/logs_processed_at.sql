-- Migração manual: garantir coluna logs.processed_at sem quebrar tabela com dados existentes.
-- Objetivo:
-- 1) criar coluna (se não existir)
-- 2) preencher nulos (backfill)
-- 3) setar NOT NULL e default

BEGIN;

ALTER TABLE logs
  ADD COLUMN IF NOT EXISTS processed_at timestamptz;

-- Backfill: caso existam linhas antigas
UPDATE logs
SET processed_at = COALESCE(processed_at, NOW())
WHERE processed_at IS NULL;

ALTER TABLE logs
  ALTER COLUMN processed_at SET DEFAULT NOW();

ALTER TABLE logs
  ALTER COLUMN processed_at SET NOT NULL;

COMMIT;

