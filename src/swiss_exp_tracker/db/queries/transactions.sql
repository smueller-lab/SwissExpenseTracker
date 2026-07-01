-- name: create_ingested_files_table#
-- Create the ingested_files table if it does not exist.
CREATE TABLE IF NOT EXISTS ingested_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    source_type TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    UNIQUE(filename, file_hash, source_type)
)

-- name: create_transactions_lnd_table#
-- Create the transactions_lnd landing table if it does not exist.
CREATE TABLE IF NOT EXISTS transactions_lnd (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (file_id) REFERENCES ingested_files(id)
)

-- name: create_transactions_raw_table#
-- Create the transactions_raw table if it does not exist.
CREATE TABLE IF NOT EXISTS transactions_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    landing_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    source_file TEXT NOT NULL,
    created_at TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (landing_id) REFERENCES transactions_lnd(id)
)

-- name: create_transactions_rfn_table#
-- Create the transactions_rfn refined table if it does not exist.
CREATE TABLE IF NOT EXISTS transactions_rfn (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    date TEXT,
    amount REAL NOT NULL,
    transaction_type TEXT NOT NULL,
    booking_text TEXT,
    merchant_normalized TEXT,
    is_person INTEGER NOT NULL DEFAULT 0,
    currency TEXT,
    reference TEXT,
    enrichment_status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    balance_chf REAL,
    FOREIGN KEY (raw_id) REFERENCES transactions_raw(id)
)

-- name: create_api_usage_table#
-- Create the api_usage table if it does not exist.
CREATE TABLE IF NOT EXISTS api_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    period TEXT NOT NULL,
    used INTEGER NOT NULL DEFAULT 0,
    credit_limit INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    UNIQUE(provider, period)
)

-- name: create_idx_ingested_files_source#
-- Create index on ingested_files(source_type) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_ingested_files_source
ON ingested_files(source_type)

-- name: create_idx_landing_file_processed#
-- Create index on transactions_lnd(file_id, processed) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_landing_file_processed
ON transactions_lnd(file_id, processed)

-- name: create_idx_raw_source_processed#
-- Create index on transactions_raw(source_type, processed) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_raw_source_processed
ON transactions_raw(source_type, processed)

-- name: create_idx_refined_enrichment_status#
-- Create index on transactions_rfn(enrichment_status) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_refined_enrichment_status
ON transactions_rfn(enrichment_status)

-- name: alter_rfn_add_is_person#
-- Add is_person column to transactions_rfn if absent.
ALTER TABLE transactions_rfn ADD COLUMN is_person INTEGER NOT NULL DEFAULT 0

-- name: alter_rfn_add_balance_chf#
-- Add balance_chf column to transactions_rfn if absent.
ALTER TABLE transactions_rfn ADD COLUMN balance_chf REAL

-- name: alter_transactions_use_add_balance_chf#
-- Add balance_chf column to transactions_use if absent.
ALTER TABLE transactions_use ADD COLUMN balance_chf REAL

-- name: get_rfn_column_names
-- Return column metadata for transactions_rfn.
SELECT * FROM pragma_table_info('transactions_rfn')

-- name: get_use_column_names
-- Return column metadata for transactions_use.
SELECT * FROM pragma_table_info('transactions_use')

-- name: get_known_files_for_source
-- Return filename and file_hash for all ingested files of the given source_type.
SELECT filename, file_hash
FROM ingested_files
WHERE source_type = :source_type

-- name: insert_ingested_file!
-- Insert a new ingested file record, ignoring conflicts.
INSERT OR IGNORE INTO ingested_files (
    filename,
    file_hash,
    source_type,
    ingested_at,
    record_count,
    status
)
VALUES (:filename, :file_hash, :source_type, :ingested_at, :record_count, :status)

-- name: get_latest_ingested_file_id$
-- Return the most recent id for the given filename and source_type.
SELECT id
FROM ingested_files
WHERE filename = :filename AND source_type = :source_type
ORDER BY id DESC
LIMIT 1

-- name: set_ingested_file_status!
-- Update the status of an ingested file by id.
UPDATE ingested_files SET status = :status WHERE id = :file_id

-- name: insert_transactions_lnd*!
-- Batch-insert rows into the landing table with processed = 0.
INSERT INTO transactions_lnd (
    file_id,
    source_type,
    raw_json,
    created_at,
    processed
)
VALUES (:file_id, :source_type, :raw_json, :created_at, 0)

-- name: get_unprocessed_landing_rows
-- Return all unprocessed landing rows for the given source_type, ordered by id.
SELECT tl.id, tl.file_id, tl.source_type, tl.raw_json, f.filename
FROM transactions_lnd tl
JOIN ingested_files f ON f.id = tl.file_id
WHERE tl.processed = 0
  AND tl.source_type = f.source_type
  AND tl.source_type = :source_type
ORDER BY tl.id

-- name: count_unprocessed_lnd_for_file$
-- Count unprocessed landing rows for a specific file_id.
SELECT COUNT(*) FROM transactions_lnd WHERE file_id = :file_id AND processed = 0

-- name: mark_lnd_processed!
-- Mark a single landing row as processed.
UPDATE transactions_lnd SET processed = 1 WHERE id = :landing_id

-- name: insert_transactions_raw!
-- Insert a single validated row into transactions_raw with processed = 0.
INSERT INTO transactions_raw (
    landing_id,
    source_type,
    raw_json,
    source_file,
    created_at,
    processed
)
VALUES (:landing_id, :source_type, :raw_json, :source_file, :created_at, 0)

-- name: mark_raw_processed!
-- Mark a single raw row as processed.
UPDATE transactions_raw SET processed = 1 WHERE id = :raw_id

-- name: count_unprocessed_raw_for_file$
-- Count unprocessed raw rows belonging to a given file_id via landing join.
SELECT COUNT(*)
FROM transactions_raw tr
JOIN transactions_lnd tl ON tl.id = tr.landing_id
WHERE tl.file_id = :file_id AND tr.processed = 0

-- name: get_unprocessed_raw_rows
-- Return all unprocessed raw rows for the given source_type, ordered by id.
SELECT tr.id, tr.landing_id, tl.file_id, tr.source_type, tr.raw_json, tr.source_file
FROM transactions_raw tr
JOIN transactions_lnd tl ON tl.id = tr.landing_id
WHERE tr.processed = 0
  AND tr.source_type = :source_type
ORDER BY tr.id

-- name: check_duplicate_rfn$
-- Check for an existing refined row matching reference, date (NULL-safe), and amount.
SELECT 1
FROM transactions_rfn
WHERE reference = :reference
  AND COALESCE(date, '') = COALESCE(:date, '')
  AND amount = :amount
LIMIT 1

-- name: insert_transactions_rfn!
-- Insert a single normalized row into transactions_rfn.
INSERT INTO transactions_rfn (
    raw_id,
    source_type,
    date,
    amount,
    transaction_type,
    booking_text,
    merchant_normalized,
    is_person,
    currency,
    reference,
    enrichment_status,
    created_at,
    balance_chf
)
VALUES (
    :raw_id,
    :source_type,
    :date,
    :amount,
    :transaction_type,
    :booking_text,
    :merchant_normalized,
    :is_person,
    :currency,
    :reference,
    :enrichment_status,
    :created_at,
    :balance_chf
)

-- name: get_rfn_rows_for_balance_backfill
-- Return balance-carrying debit refined rows missing balance_chf, joined with raw JSON.
SELECT rfn.id, rfn.source_type, raw.raw_json
FROM transactions_rfn rfn
JOIN transactions_raw raw ON raw.id = rfn.raw_id
WHERE rfn.balance_chf IS NULL
  AND rfn.source_type IN ('ZKB_DEBIT', 'UBS_DEBIT')

-- name: set_rfn_balance_chf!
-- Update balance_chf for a single refined row by id.
UPDATE transactions_rfn SET balance_chf = :balance_chf WHERE id = :rfn_id

-- name: get_rfn_rows_missing_booking_text
-- Return refined rows with a NULL/empty booking_text, joined with their raw JSON.
SELECT rfn.id, rfn.source_type, raw.raw_json
FROM transactions_rfn rfn
JOIN transactions_raw raw ON raw.id = rfn.raw_id
WHERE rfn.booking_text IS NULL
   OR TRIM(rfn.booking_text) = ''

-- name: set_rfn_booking_text!
-- Update booking_text, merchant_normalized and is_person for a single refined row by id.
UPDATE transactions_rfn
SET booking_text = :booking_text,
    merchant_normalized = :merchant_normalized,
    is_person = :is_person
WHERE id = :rfn_id

-- name: get_rfn_rows_by_source_and_type
-- Return id/amount/date/booking_text for a source and transaction type, ordered for matching.
SELECT id, amount, date, booking_text
FROM transactions_rfn
WHERE source_type = :source_type
  AND transaction_type = :transaction_type
ORDER BY amount, COALESCE(date, ''), id

-- name: delete_rfn_by_source_type_and_merchant_substr!
-- Delete refined rows for a source/type whose merchant_normalized contains the substring.
DELETE FROM transactions_rfn
WHERE source_type = :source_type
  AND transaction_type = :transaction_type
  AND merchant_normalized IS NOT NULL
  AND lower(merchant_normalized) LIKE :merchant_pattern

-- name: fill_viseca_fee_text!
-- Set booking_text and merchant_normalized on matching Viseca fee rows.
UPDATE transactions_rfn
SET booking_text = :booking_text,
    merchant_normalized = :merchant_normalized
WHERE source_type = :source_type
  AND transaction_type = :transaction_type
  AND amount = :amount
  AND (
        booking_text IS NULL
     OR TRIM(booking_text) = ''
  )
  AND merchant_normalized IS NULL

-- name: get_api_usage$
-- Return the stored used count for a provider and period.
SELECT used FROM api_usage WHERE provider = :provider AND period = :period

-- name: upsert_api_usage!
-- Insert or update the api_usage record for a provider and period.
INSERT INTO api_usage (provider, period, used, credit_limit, updated_at)
VALUES (:provider, :period, :used, :credit_limit, :updated_at)
ON CONFLICT(provider, period) DO UPDATE SET
    used = excluded.used,
    credit_limit = excluded.credit_limit,
    updated_at = excluded.updated_at

-- name: get_transactions_use
-- Select all rows from transactions_use.
SELECT * FROM transactions_use

-- name: get_dash_balance
-- Select all rows from dash_balance.
SELECT * FROM dash_balance

-- name: get_dash_groceries
-- Select all rows from dash_groceries.
SELECT * FROM dash_groceries

-- name: get_dash_food
-- Select all rows from dash_food.
SELECT * FROM dash_food

-- name: get_dash_cat_main
-- Select all rows from dash_cat_main.
SELECT * FROM dash_cat_main

-- name: get_dash_stats
-- Select all rows from dash_stats.
SELECT * FROM dash_stats

-- name: get_dash_top_expenses
-- Select all rows from dash_top_expenses.
SELECT * FROM dash_top_expenses

-- name: get_dash_net_balance_month
-- Select Month, expense, income, NetBalance from dash_net_balance_month.
SELECT Month, expense, income, NetBalance FROM dash_net_balance_month

-- name: get_dash_vacation
-- Select all rows from dash_vacation.
SELECT * FROM dash_vacation

-- name: get_dash_vacation_transactions
-- Select all rows from dash_vacation_transactions.
SELECT * FROM dash_vacation_transactions

-- name: get_dash_transport
-- Select all rows from dash_transport.
SELECT * FROM dash_transport

-- name: get_dash_transport_heatmap
-- Select all rows from dash_transport_heatmap.
SELECT * FROM dash_transport_heatmap

-- name: get_dash_sport
-- Select all rows from dash_sport.
SELECT * FROM dash_sport

-- name: get_dash_sport_activities
-- Select all rows from dash_sport_activities.
SELECT * FROM dash_sport_activities

-- name: get_dash_car
-- Select all rows from dash_car.
SELECT * FROM dash_car

-- name: get_dash_retail
-- Select all rows from dash_retail.
SELECT * FROM dash_retail

-- name: get_dash_retail_donut
-- Select all rows from dash_retail_donut.
SELECT * FROM dash_retail_donut

-- name: get_dash_retail_top
-- Select all rows from dash_retail_top.
SELECT * FROM dash_retail_top

-- name: get_groceries_use
-- Select all rows from groceries_use.
SELECT * FROM groceries_use

-- name: get_dash_groceries_cat
-- Select all rows from dash_groceries_cat.
SELECT * FROM dash_groceries_cat

-- name: get_dash_groceries_health
-- Select all rows from dash_groceries_health.
SELECT * FROM dash_groceries_health

-- name: get_dash_groceries_top_articles
-- Select all rows from dash_groceries_top_articles.
SELECT * FROM dash_groceries_top_articles

-- name: get_dash_top_category
-- Select all rows from dash_top_category.
SELECT * FROM dash_top_category

-- name: backfill_transactions_use_balance_chf!
-- Backfill balance_chf in transactions_use from transactions_rfn by reference.
UPDATE transactions_use
SET balance_chf = (
    SELECT t.balance_chf FROM transactions_rfn t
    WHERE t.reference = transactions_use.reference
)
WHERE balance_chf IS NULL

-- name: get_dash_balance_sheet
-- Select all rows from dash_balance_sheet.
SELECT * FROM dash_balance_sheet

-- name: get_dash_balance_sheet_categories
-- Select all rows from dash_balance_sheet_categories.
SELECT * FROM dash_balance_sheet_categories
