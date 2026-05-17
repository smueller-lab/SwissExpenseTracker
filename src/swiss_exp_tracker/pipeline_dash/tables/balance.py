from __future__ import annotations

import sqlite3

import pandas as pd


def build(df: pd.DataFrame, con: sqlite3.Connection) -> None:
    mask = (df["source_type"] == "ZKB_DEBIT") & df["balance_chf"].notna()
    df_bal = df[mask].copy()

    df_bal = (
        df_bal.sort_values("date").groupby("date", as_index=False)["balance_chf"].last()
    )

    cutoff = df_bal["date"].max() - pd.DateOffset(years=1)
    df_bal = df_bal[df_bal["date"] >= cutoff].reset_index(drop=True)

    df_bal.to_sql("dash_balance", con, if_exists="replace", index=False)
