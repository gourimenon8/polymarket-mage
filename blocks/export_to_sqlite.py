import sqlite3
import pandas as pd
from mage_ai.data_preparation.decorators import data_exporter, test


DB_PATH = "prediction_markets.db"

EXPORT_COLUMNS = [
    "id", "question", "yes_price", "no_price", "yes_label",
    "volume", "liquidity", "days_remaining", "certainty_score",
    "near_certain", "vol_liq_anomaly", "expiring_toss_up",
    "anomaly_score", "is_anomaly", "anomaly_reason",
    "llm_category", "surprise_score", "llm_context",
    "ingested_at",
]


@data_exporter
def export_to_sqlite(df, *args, **kwargs):
    """
    Persist enriched market data to SQLite.
    Two tables:
    - markets        : latest snapshot (replaced on each run)
    - markets_history: append-only log across runs
    """
    export_df = df[[c for c in EXPORT_COLUMNS if c in df.columns]].copy()

    # Ensure boolean columns serialize cleanly
    bool_cols = ["near_certain", "vol_liq_anomaly", "expiring_toss_up", "is_anomaly"]
    for col in bool_cols:
        if col in export_df.columns:
            export_df[col] = export_df[col].astype(int)

    conn = sqlite3.connect(DB_PATH)

    # Latest snapshot
    export_df.to_sql("markets", conn, if_exists="replace", index=False)

    # Append to history
    export_df.to_sql("markets_history", conn, if_exists="append", index=False)

    # Quick summary query
    summary = pd.read_sql(
        """
        SELECT
            COUNT(*) as total_markets,
            SUM(is_anomaly) as anomalies_flagged,
            ROUND(AVG(yes_price), 3) as avg_yes_price,
            ROUND(SUM(volume), 0) as total_volume
        FROM markets
        """,
        conn,
    )

    conn.close()

    print(f"\nExported {len(export_df)} markets to {DB_PATH}")
    print("\nRun summary:")
    print(summary.to_string(index=False))

    return df


@test
def test_output(df) -> None:
    conn = sqlite3.connect(DB_PATH)
    count = pd.read_sql("SELECT COUNT(*) as n FROM markets", conn).iloc[0]["n"]
    conn.close()
    assert count > 0, "No rows written to SQLite"
