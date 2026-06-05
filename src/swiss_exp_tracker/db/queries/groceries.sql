-- name: create_groceries_lnd_table#
-- Create groceries_lnd table if it does not exist.
CREATE TABLE IF NOT EXISTS groceries_lnd (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id      INTEGER NOT NULL,
    source_type  TEXT NOT NULL,
    raw_json     TEXT NOT NULL,
    is_bonus_row INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    processed    INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (file_id) REFERENCES ingested_files(id)
)

-- name: create_groceries_raw_table#
-- Create groceries_raw table if it does not exist.
CREATE TABLE IF NOT EXISTS groceries_raw (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    landing_id   INTEGER NOT NULL,
    source_type  TEXT NOT NULL,
    raw_json     TEXT NOT NULL,
    source_file  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    processed    INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (landing_id) REFERENCES groceries_lnd(id)
)

-- name: create_groceries_rfn_table#
-- Create groceries_rfn table if it does not exist.
CREATE TABLE IF NOT EXISTS groceries_rfn (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER,
    source_type         TEXT NOT NULL,
    date                TEXT NOT NULL,
    time                TEXT NOT NULL,
    location            TEXT NOT NULL,
    article             TEXT NOT NULL,
    article_normalized  TEXT NOT NULL,
    unit                TEXT NOT NULL,
    quantity            REAL NOT NULL,
    price_chf           REAL NOT NULL,
    discount_chf        REAL NOT NULL,
    enrichment_status   TEXT NOT NULL DEFAULT 'pending',
    created_at          TEXT NOT NULL,
    FOREIGN KEY (raw_id) REFERENCES groceries_raw(id)
)

-- name: create_groceries_use_table#
-- Create groceries_use table if it does not exist.
CREATE TABLE IF NOT EXISTS groceries_use (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rfn_id          INTEGER NOT NULL,
    date            TEXT NOT NULL,
    time            TEXT NOT NULL,
    location        TEXT NOT NULL,
    article         TEXT NOT NULL,
    unit            TEXT NOT NULL,
    quantity        REAL NOT NULL,
    price_chf       REAL NOT NULL,
    discount_chf    REAL NOT NULL,
    category_main   TEXT NOT NULL,
    category_detail TEXT NOT NULL
)

-- name: create_idx_groceries_lnd_processed#
-- Create index on groceries_lnd(processed).
CREATE INDEX IF NOT EXISTS idx_groceries_lnd_processed
ON groceries_lnd(processed)

-- name: create_idx_groceries_raw_processed#
-- Create index on groceries_raw(processed).
CREATE INDEX IF NOT EXISTS idx_groceries_raw_processed
ON groceries_raw(processed)

-- name: create_idx_groceries_rfn_enrichment#
-- Create index on groceries_rfn(enrichment_status).
CREATE INDEX IF NOT EXISTS idx_groceries_rfn_enrichment
ON groceries_rfn(enrichment_status)

-- name: create_idx_groceries_rfn_dedup#
-- Create composite index to accelerate duplicate checks in stage_03_rfn.
CREATE INDEX IF NOT EXISTS idx_groceries_rfn_dedup
ON groceries_rfn(date, time, location, article, quantity)

-- name: create_grocery_categorization_raw_table#
-- Create grocery_categorization_raw table if it does not exist.
CREATE TABLE IF NOT EXISTS grocery_categorization_raw (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    rfn_id          INTEGER NOT NULL,
    article         TEXT NOT NULL,
    matched_article TEXT NOT NULL,
    cache_hit       INTEGER NOT NULL,
    similarity      REAL,
    category_main   TEXT NOT NULL,
    category_detail TEXT NOT NULL
)

-- name: create_grocery_categorization_rfn_view#
-- Create view that returns the latest categorization row per rfn_id.
CREATE VIEW IF NOT EXISTS grocery_categorization_rfn AS
SELECT * FROM grocery_categorization_raw
WHERE id = (
    SELECT MAX(id)
    FROM grocery_categorization_raw g2
    WHERE g2.rfn_id = grocery_categorization_raw.rfn_id
)

-- name: insert_groceries_lnd*!
-- Batch-insert rows into groceries_lnd.
INSERT INTO groceries_lnd
    (file_id, source_type, raw_json, is_bonus_row, created_at, processed)
VALUES (:file_id, :source_type, :raw_json, :is_bonus_row, :created_at, 0)

-- name: count_unprocessed_groceries_lnd_for_file$
-- Count unprocessed groceries_lnd rows for a given file_id.
SELECT COUNT(*) FROM groceries_lnd WHERE file_id = :file_id AND processed = 0

-- name: mark_groceries_lnd_processed!
-- Mark a single groceries_lnd row as processed.
UPDATE groceries_lnd SET processed = 1 WHERE id = :landing_id

-- name: get_unprocessed_groceries_lnd_rows
-- Return all unprocessed groceries_lnd rows for a given source_type, ordered by id.
SELECT gl.id, gl.file_id, gl.raw_json, gl.is_bonus_row, f.filename
FROM groceries_lnd gl
JOIN ingested_files f ON f.id = gl.file_id
WHERE gl.processed = 0
  AND gl.source_type = :source_type
ORDER BY gl.id

-- name: insert_groceries_raw!
-- Insert a single row into groceries_raw.
INSERT INTO groceries_raw
    (landing_id, source_type, raw_json, source_file, created_at, processed)
VALUES (:landing_id, :source_type, :raw_json, :source_file, :created_at, 0)

-- name: count_unprocessed_groceries_raw_for_file$
-- Count unprocessed groceries_lnd rows for a given file_id (raw stage check).
SELECT COUNT(*) FROM groceries_lnd WHERE file_id = :file_id AND processed = 0

-- name: get_unprocessed_groceries_raw_rows
-- Return all unprocessed groceries_raw rows for a given source_type, ordered by id.
SELECT gr.id, gr.landing_id, gl.file_id, gr.raw_json, gr.source_file
FROM groceries_raw gr
JOIN groceries_lnd gl ON gl.id = gr.landing_id
WHERE gr.processed = 0
  AND gr.source_type = :source_type
ORDER BY gr.id

-- name: check_duplicate_groceries_rfn$
-- Return 1 if a matching row already exists in groceries_rfn, else NULL.
SELECT 1 FROM groceries_rfn
WHERE date = :date AND time = :time AND location = :location AND article = :article AND quantity = :quantity
LIMIT 1

-- name: insert_groceries_rfn!
-- Insert a single refined grocery row into groceries_rfn.
INSERT INTO groceries_rfn (
    raw_id, source_type, date, time, location,
    article, article_normalized, unit,
    quantity, price_chf, discount_chf,
    enrichment_status, created_at
) VALUES (:raw_id, :source_type, :date, :time, :location,
          :article, :article_normalized, :unit,
          :quantity, :price_chf, :discount_chf, 'pending', :created_at)

-- name: mark_groceries_raw_processed!
-- Mark a single groceries_raw row as processed.
UPDATE groceries_raw SET processed = 1 WHERE id = :raw_id

-- name: get_file_id_for_groceries_raw_row$
-- Return the file_id for a given groceries_raw row by joining through groceries_lnd.
SELECT gl.file_id
FROM groceries_raw gr
JOIN groceries_lnd gl ON gl.id = gr.landing_id
WHERE gr.id = :raw_id

-- name: count_unprocessed_groceries_raw_for_file_full$
-- Count unprocessed groceries_raw rows for a given file_id via join to groceries_lnd.
SELECT COUNT(*) FROM groceries_raw gr
JOIN groceries_lnd gl ON gl.id = gr.landing_id
WHERE gl.file_id = :file_id AND gr.processed = 0

-- name: get_groceries_for_use
-- Return enriched grocery rows not yet present in groceries_use, ordered newest first.
SELECT
    r.id          AS rfn_id,
    r.date,
    r.time,
    r.location,
    r.article,
    r.unit,
    r.quantity,
    r.price_chf,
    r.discount_chf,
    c.category_main,
    c.category_detail
FROM groceries_rfn r
JOIN grocery_categorization_rfn c ON c.rfn_id = r.id
WHERE r.enrichment_status = 'enriched'
  AND r.id NOT IN (SELECT rfn_id FROM groceries_use)
ORDER BY r.date DESC, r.time DESC

-- name: insert_groceries_use!
-- Insert a single row into groceries_use.
INSERT INTO groceries_use (
    rfn_id, date, time, location, article,
    unit, quantity, price_chf, discount_chf,
    category_main, category_detail
) VALUES (:rfn_id, :date, :time, :location, :article,
          :unit, :quantity, :price_chf, :discount_chf,
          :category_main, :category_detail)

-- name: apply_groceries_use_category_correction!
-- Update category_main and category_detail for groceries_use rows matching a LIKE pattern.
UPDATE groceries_use
   SET category_main   = :category_main,
       category_detail = :category_detail
 WHERE article LIKE :pattern
   AND (category_main != :category_main OR category_detail != :category_detail)

-- name: get_pending_groceries
-- Return all groceries_rfn rows with enrichment_status = 'pending', ordered by id.
SELECT id, article, article_normalized, location
FROM groceries_rfn
WHERE enrichment_status = 'pending'
ORDER BY id

-- name: insert_grocery_categorization_raw!
-- Insert a single categorization result into grocery_categorization_raw.
INSERT INTO grocery_categorization_raw (
    created_at, rfn_id, article, matched_article,
    cache_hit, similarity, category_main, category_detail
) VALUES (:created_at, :rfn_id, :article, :matched_article,
          :cache_hit, :similarity, :category_main, :category_detail)

-- name: mark_grocery_enriched!
-- Set enrichment_status = 'enriched' for a groceries_rfn row.
UPDATE groceries_rfn SET enrichment_status = 'enriched' WHERE id = :rfn_id
