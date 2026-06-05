-- name: create_merchant_metadata_raw_table#
-- Create the merchant_metadata_raw table if it does not exist.
CREATE TABLE IF NOT EXISTS merchant_metadata_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    reference_id TEXT,
    matched_merchant TEXT NOT NULL,
    cache_hit INTEGER NOT NULL,
    similarity REAL,
    search_tool TEXT,
    category_main TEXT,
    category_second TEXT,
    city TEXT
)

-- name: create_merchant_metadata_rfn_table#
-- Create the merchant_metadata_rfn table if it does not exist.
CREATE TABLE IF NOT EXISTS merchant_metadata_rfn (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    reference_id TEXT,
    matched_merchant TEXT NOT NULL,
    cache_hit INTEGER NOT NULL,
    similarity REAL,
    search_tool TEXT,
    category_main TEXT,
    category_second TEXT,
    city TEXT
)

-- name: create_transactions_use_table#
-- Create the transactions_use table if it does not exist.
CREATE TABLE IF NOT EXISTS transactions_use (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    date TEXT,
    amount REAL NOT NULL,
    transaction_type TEXT NOT NULL,
    currency TEXT,
    reference TEXT,
    merchant TEXT NOT NULL,
    category_main TEXT NOT NULL,
    category_second TEXT,
    city TEXT,
    balance_chf REAL
)

-- name: alter_transactions_use_add_balance_chf#
-- Add the balance_chf column to transactions_use if it is missing.
ALTER TABLE transactions_use ADD COLUMN balance_chf REAL

-- name: get_pending_transactions
-- Return all transactions_rfn rows where enrichment_status is pending, ordered by created_at.
SELECT
    id,
    date,
    merchant_normalized,
    booking_text,
    reference,
    amount,
    is_person
FROM transactions_rfn
WHERE enrichment_status = 'pending'
ORDER BY created_at ASC

-- name: mark_transaction_enriched!
-- Set enrichment_status to enriched for the given transactions_rfn row id.
UPDATE transactions_rfn
SET enrichment_status = 'enriched'
WHERE id = :refined_id

-- name: insert_merchant_metadata_raw!
-- Insert one raw merchant metadata result row.
INSERT INTO merchant_metadata_raw (
    created_at,
    reference_id,
    matched_merchant,
    cache_hit,
    similarity,
    search_tool,
    category_main,
    category_second,
    city
)
VALUES (
    :created_at,
    :reference_id,
    :matched_merchant,
    :cache_hit,
    :similarity,
    :search_tool,
    :category_main,
    :category_second,
    :city
)

-- name: check_merchant_metadata_raw_exists$
-- Return 1 if the merchant_metadata_raw table exists, else no row.
SELECT 1 FROM sqlite_master WHERE type='table' AND name='merchant_metadata_raw'

-- name: check_merchant_metadata_rfn_exists$
-- Return 1 if the merchant_metadata_rfn table exists, else no row.
SELECT 1 FROM sqlite_master WHERE type='table' AND name='merchant_metadata_rfn'

-- name: get_latest_raw_merchant_rows
-- Return the latest raw row per reference_id that is new or newer than the existing rfn row.
SELECT r.*
FROM merchant_metadata_raw r
INNER JOIN (
    SELECT reference_id, MAX(id) AS max_id
    FROM merchant_metadata_raw
    GROUP BY reference_id
) latest ON r.id = latest.max_id
WHERE r.reference_id NOT IN (
    SELECT reference_id
    FROM merchant_metadata_rfn
    WHERE reference_id IS NOT NULL
)
OR r.created_at > (
    SELECT MAX(rfn.created_at)
    FROM merchant_metadata_rfn rfn
    WHERE rfn.reference_id = r.reference_id
)
ORDER BY r.created_at DESC

-- name: delete_merchant_metadata_rfn_by_reference!
-- Delete the merchant_metadata_rfn row for the given reference_id.
DELETE FROM merchant_metadata_rfn WHERE reference_id = :reference_id

-- name: insert_merchant_metadata_rfn!
-- Insert one refined merchant metadata row.
INSERT INTO merchant_metadata_rfn (
    created_at,
    reference_id,
    matched_merchant,
    cache_hit,
    similarity,
    search_tool,
    category_main,
    category_second,
    city
)
VALUES (
    :created_at,
    :reference_id,
    :matched_merchant,
    :cache_hit,
    :similarity,
    :search_tool,
    :category_main,
    :category_second,
    :city
)

-- name: get_all_merchant_metadata_rfn
-- Return all rows from merchant_metadata_rfn.
SELECT * FROM merchant_metadata_rfn

-- name: update_merchant_metadata_rfn_categories!
-- Update category_main, category_second, and city for the given merchant_metadata_rfn row id.
UPDATE merchant_metadata_rfn
SET category_main = :category_main,
    category_second = :category_second,
    city = :city
WHERE id = :id

-- name: update_merchant_metadata_rfn_full_by_reference!
-- Update matched_merchant, category_main, category_second, and city by reference_id.
UPDATE merchant_metadata_rfn
SET matched_merchant = :matched_merchant,
    category_main = :category_main,
    category_second = :category_second,
    city = :city
WHERE reference_id = :reference_id

-- name: update_transactions_use_merchant_by_reference!
-- Update merchant, category_main, category_second, and city for the transactions_use row matching the given reference.
UPDATE transactions_use
SET merchant = :merchant,
    category_main = :category_main,
    category_second = :category_second,
    city = :city
WHERE reference = :reference

-- name: get_transactions_use_column_names
-- Return column metadata for transactions_use.
SELECT * FROM pragma_table_info('transactions_use')

-- name: get_enriched_transactions_not_in_use
-- Return enriched transactions joined with rfn merchant data that are not yet in transactions_use.
SELECT
    t.id              AS transaction_id,
    t.source_type,
    t.date,
    t.amount,
    t.transaction_type,
    t.currency,
    t.reference,
    m.matched_merchant  AS merchant,
    m.category_main,
    m.category_second,
    m.city,
    t.balance_chf
FROM transactions_rfn t
JOIN merchant_metadata_rfn m ON m.reference_id = t.reference
WHERE t.enrichment_status = 'enriched'
  AND t.reference NOT IN (
      SELECT reference FROM transactions_use
      WHERE reference IS NOT NULL
  )
ORDER BY t.date DESC

-- name: insert_transactions_use*!
-- Batch-insert rows into transactions_use.
INSERT INTO transactions_use (
    transaction_id, source_type, date, amount, transaction_type,
    currency, reference,
    merchant, category_main, category_second, city, balance_chf
) VALUES (
    :transaction_id, :source_type, :date, :amount, :transaction_type,
    :currency, :reference,
    :merchant, :category_main, :category_second, :city, :balance_chf
)

-- name: get_transactions_use_sync_candidates
-- Return transactions_use rows whose categories differ from the current rfn values.
SELECT tu.id, m.category_main, m.category_second, m.city
FROM transactions_use tu
JOIN merchant_metadata_rfn m ON m.reference_id = tu.reference
WHERE tu.category_main != m.category_main
   OR COALESCE(tu.category_second, '') != COALESCE(m.category_second, '')
   OR COALESCE(tu.city, '')            != COALESCE(m.city, '')

-- name: update_transactions_use_categories!
-- Update category_main, category_second, and city for the given transactions_use row id.
UPDATE transactions_use
SET category_main = :category_main,
    category_second = :category_second,
    city = :city
WHERE id = :id

-- name: get_all_transactions_use_for_corrections
-- Return id, merchant, amount, and category columns from transactions_use for correction logic.
SELECT id, merchant, amount, category_main, category_second FROM transactions_use

-- name: update_transactions_use_category_correction!
-- Overwrite category_main and category_second for the given transactions_use row id.
UPDATE transactions_use
SET category_main = :category_main,
    category_second = :category_second
WHERE id = :id

-- name: delete_transactions_use_by_id!
-- Delete the transactions_use row with the given id.
DELETE FROM transactions_use WHERE id = :id

-- name: update_transactions_use_amount!
-- Update the amount field for the given transactions_use row id.
UPDATE transactions_use SET amount = :amount WHERE id = :id

-- name: get_transactions_use_income_by_amount
-- Return id and date for INCOME rows in transactions_use matching the given amount.
SELECT id, date FROM transactions_use
WHERE amount = :amount
  AND transaction_type = 'INCOME'

-- name: get_api_usage$
-- Return the stored used count for a provider and period, or no row if absent.
SELECT used FROM api_usage WHERE provider = :provider AND period = :period

-- name: upsert_api_usage!
-- Insert or update the api_usage row for the given provider and period.
INSERT INTO api_usage (provider, period, used, credit_limit, updated_at)
VALUES (:provider, :period, :used, :credit_limit, :updated_at)
ON CONFLICT(provider, period) DO UPDATE SET
    used = excluded.used,
    credit_limit = excluded.credit_limit,
    updated_at = excluded.updated_at
