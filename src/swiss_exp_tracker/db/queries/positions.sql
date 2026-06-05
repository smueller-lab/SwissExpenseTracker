-- name: create_ingested_files_pos_table#
-- Create ingested_files_pos table if it does not exist.
CREATE TABLE IF NOT EXISTS ingested_files_pos (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT    NOT NULL,
    file_hash    TEXT    NOT NULL,
    ingested_at  TEXT    NOT NULL,
    record_count INTEGER NOT NULL,
    status       TEXT    NOT NULL,
    UNIQUE(filename, file_hash)
)

-- name: create_positions_lnd_table#
-- Create positions_lnd table if it does not exist.
CREATE TABLE IF NOT EXISTS positions_lnd (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id    INTEGER NOT NULL,
    raw_json   TEXT    NOT NULL,
    created_at TEXT    NOT NULL,
    processed  INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (file_id) REFERENCES ingested_files_pos(id)
)

-- name: create_positions_raw_table#
-- Create positions_raw table if it does not exist.
CREATE TABLE IF NOT EXISTS positions_raw (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    landing_id  INTEGER NOT NULL,
    raw_json    TEXT    NOT NULL,
    source_file TEXT    NOT NULL,
    created_at  TEXT    NOT NULL,
    processed   INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (landing_id) REFERENCES positions_lnd(id)
)

-- name: create_positions_rfn_table#
-- Create positions_rfn table if it does not exist.
CREATE TABLE IF NOT EXISTS positions_rfn (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date       TEXT    NOT NULL,
    account_id          TEXT    NOT NULL,
    asset_class         TEXT    NOT NULL,
    symbol              TEXT    NOT NULL,
    name                TEXT,
    quantity            REAL    NOT NULL,
    unit_cost           REAL,
    price               REAL    NOT NULL,
    currency            TEXT    NOT NULL,
    total_value         REAL,
    total_value_chf     REAL    NOT NULL,
    pnl_nominal_chf     REAL,
    pnl_pct_chf         REAL,
    position_weight_pct REAL,
    source_file         TEXT    NOT NULL,
    created_at          TEXT    NOT NULL,
    UNIQUE(snapshot_date, symbol, account_id)
)

-- name: create_positions_use_table#
-- Create positions_use table if it does not exist.
CREATE TABLE IF NOT EXISTS positions_use (
    date        TEXT NOT NULL,
    account     TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    name        TEXT,
    quantity    REAL NOT NULL,
    unit_cost   REAL,
    price       REAL NOT NULL,
    currency    TEXT NOT NULL,
    value       REAL,
    value_chf   REAL NOT NULL,
    pnl_chf     REAL,
    pnl_pct     REAL,
    weight_pct  REAL
)

-- name: create_idx_pos_rfn_date#
-- Create index on positions_rfn(snapshot_date) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_pos_rfn_date
ON positions_rfn(snapshot_date)

-- name: create_idx_pos_rfn_symbol#
-- Create index on positions_rfn(symbol) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_pos_rfn_symbol
ON positions_rfn(symbol)

-- name: create_idx_pos_lnd_processed#
-- Create index on positions_lnd(file_id, processed) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_pos_lnd_processed
ON positions_lnd(file_id, processed)

-- name: create_idx_pos_raw_processed#
-- Create index on positions_raw(processed) if it does not exist.
CREATE INDEX IF NOT EXISTS idx_pos_raw_processed
ON positions_raw(processed)

-- name: get_known_positions_files
-- Select filename and file_hash for all ingested positions files.
SELECT filename, file_hash FROM ingested_files_pos

-- name: insert_ingested_file_pos!
-- Insert a new ingested positions file record, ignoring duplicates.
INSERT OR IGNORE INTO ingested_files_pos (filename, file_hash, ingested_at, record_count, status)
VALUES (:filename, :file_hash, :ingested_at, :record_count, :status)

-- name: get_latest_positions_file_id$
-- Return the latest id for the given filename in ingested_files_pos.
SELECT id FROM ingested_files_pos WHERE filename = :filename ORDER BY id DESC LIMIT 1

-- name: set_positions_file_status!
-- Update the status of a positions file record by id.
UPDATE ingested_files_pos SET status = :status WHERE id = :file_id

-- name: insert_positions_lnd*!
-- Batch-insert raw JSON rows into positions_lnd with processed=0.
INSERT INTO positions_lnd (file_id, raw_json, created_at, processed)
VALUES (:file_id, :raw_json, :created_at, 0)

-- name: get_unprocessed_positions_lnd_rows
-- Select all unprocessed landing rows joined with their source filename.
SELECT pl.id, pl.file_id, pl.raw_json, f.filename
FROM positions_lnd pl
JOIN ingested_files_pos f ON f.id = pl.file_id
WHERE pl.processed = 0
ORDER BY pl.id

-- name: mark_positions_lnd_processed!
-- Mark a single landing row as processed.
UPDATE positions_lnd SET processed = 1 WHERE id = :landing_id

-- name: count_unprocessed_positions_lnd_for_file$
-- Count unprocessed landing rows for a given file_id.
SELECT COUNT(*) FROM positions_lnd WHERE file_id = :file_id AND processed = 0

-- name: insert_positions_raw!
-- Insert a validated raw JSON row into positions_raw with processed=0.
INSERT INTO positions_raw (landing_id, raw_json, source_file, created_at, processed)
VALUES (:landing_id, :raw_json, :source_file, :created_at, 0)

-- name: mark_positions_raw_processed!
-- Mark a single raw row as processed.
UPDATE positions_raw SET processed = 1 WHERE id = :raw_id

-- name: count_unprocessed_positions_raw_for_file$
-- Count unprocessed raw rows for a given file_id via landing join.
SELECT COUNT(*) FROM positions_raw pr
JOIN positions_lnd pl ON pl.id = pr.landing_id
WHERE pl.file_id = :file_id AND pr.processed = 0

-- name: get_unprocessed_positions_raw_rows
-- Select all unprocessed raw rows joined with their file_id.
SELECT pr.id, pl.file_id, pr.raw_json, pr.source_file
FROM positions_raw pr
JOIN positions_lnd pl ON pl.id = pr.landing_id
WHERE pr.processed = 0
ORDER BY pr.id

-- name: insert_positions_rfn_ignore!
-- Insert a refined position record, ignoring duplicates by (snapshot_date, symbol, account_id).
INSERT OR IGNORE INTO positions_rfn (
    snapshot_date, account_id, asset_class,
    symbol, name, quantity, unit_cost, price, currency,
    total_value, total_value_chf, pnl_nominal_chf, pnl_pct_chf,
    position_weight_pct, source_file, created_at
)
VALUES (
    :snapshot_date, :account_id, :asset_class,
    :symbol, :name, :quantity, :unit_cost, :price, :currency,
    :total_value, :total_value_chf, :pnl_nominal_chf, :pnl_pct_chf,
    :position_weight_pct, :source_file, :created_at
)

-- name: delete_positions_use!
-- Delete all rows from positions_use.
DELETE FROM positions_use

-- name: populate_positions_use!
-- Populate positions_use from positions_rfn ordered by snapshot_date, asset_class, symbol.
INSERT INTO positions_use (
    date, account, asset_class,
    symbol, name, quantity, unit_cost, price, currency,
    value, value_chf, pnl_chf, pnl_pct, weight_pct
)
SELECT
    snapshot_date, account_id, asset_class,
    symbol, name, quantity, unit_cost, price, currency,
    total_value, total_value_chf, pnl_nominal_chf, pnl_pct_chf,
    position_weight_pct
FROM positions_rfn
ORDER BY snapshot_date, asset_class, symbol

-- name: get_positions_use
-- Select all rows from positions_use.
SELECT * FROM positions_use
