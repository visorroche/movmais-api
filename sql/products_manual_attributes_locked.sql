DO $$ BEGIN
  ALTER TABLE "products" ADD COLUMN "manual_attributes_locked" boolean;
EXCEPTION
  WHEN duplicate_column THEN RAISE NOTICE 'column manual_attributes_locked already exists in products.';
END $$;

UPDATE "products" SET "manual_attributes_locked" = FALSE WHERE "manual_attributes_locked" IS NULL;

ALTER TABLE "products" ALTER COLUMN "manual_attributes_locked" SET NOT NULL;
ALTER TABLE "products" ALTER COLUMN "manual_attributes_locked" SET DEFAULT FALSE;

