from __future__ import annotations

import os
import sqlite3

from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MetadataResult


oj = os.path.join

__all__ = ["MerchantResult", "MetadataResult"]


class MerchantResult:
    def __init__(self) -> None:
        self.dr_db = "./database"
        self.path_db = oj(self.dr_db, "transactions.db")

        os.makedirs(self.dr_db, exist_ok=True)

    def save_merchant_result(self, merchant_result: MetadataResult) -> None:
        table_name = "Merchant_Metadata"
        with sqlite3.connect(self.path_db) as db:
            db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    zkb_reference TEXT,
                    matched_merchant TEXT NOT NULL,
                    cache_hit INTEGER NOT NULL,
                    similarity REAL,
                    search_tool TEXT,
                    category_main TEXT NOT NULL,
                    category_second TEXT NOT NULL,
                    city TEXT
                )
                """
            )

            db.execute(
                f"""
                INSERT INTO {table_name} (
                    created_at,
                    zkb_reference,
                    matched_merchant,
                    cache_hit,
                    similarity,
                    search_tool,
                    category_main,
                    category_second,
                    city
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    merchant_result.current_datetime.isoformat(),
                    merchant_result.zkb_reference,
                    merchant_result.matched_merchant,
                    int(merchant_result.cache_hit),
                    merchant_result.similarity,
                    merchant_result.search_tool,
                    merchant_result.category_main,
                    merchant_result.category_second,
                    merchant_result.city,
                ),
            )
            db.commit()
