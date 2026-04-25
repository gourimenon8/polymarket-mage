import pandas as pd
import numpy as np
from mage_ai.data_preparation.decorators import transformer, test


@transformer
def detect_anomalies(df, *args, **kwargs):
    """
    Flag statistically unusual markets across three dimensions:

    1. near_certain     — probability ≥ 0.90 or ≤ 0.10 (market approaching resolution)
    2. vol_liq_anomaly  — volume/liquidity ratio is a z-score outlier (>2σ)
                          High volume with thin liquidity = potential manipulation signal
    3. expiring_toss_up — resolves within 7 days but still near 50/50 (high uncertainty)

    composite anomaly_score = count of flags triggered (0–3)
    """

    # --- Flag 1: Near-certain outcome ---
    df["near_certain"] = df["yes_price"].apply(
        lambda x: x >= 0.90 or x <= 0.10 if pd.notnull(x) else False
    )

    # --- Flag 2: Volume/Liquidity imbalance ---
    df["vol_liq_ratio"] = df["volume"] / (df["liquidity"] + 1.0)
    ratio_mean = df["vol_liq_ratio"].mean()
    ratio_std = df["vol_liq_ratio"].std() + 1e-9
    df["vol_liq_zscore"] = (df["vol_liq_ratio"] - ratio_mean) / ratio_std
    df["vol_liq_anomaly"] = df["vol_liq_zscore"] > 2.0

    # --- Flag 3: Expiring toss-up ---
    df["expiring_toss_up"] = (
        (df["days_remaining"] <= 7) &
        (df["yes_price"].between(0.35, 0.65))
    )

    # --- Composite score and reason string ---
    df["anomaly_score"] = (
        df["near_certain"].astype(int) +
        df["vol_liq_anomaly"].astype(int) +
        df["expiring_toss_up"].astype(int)
    )

    def build_reason(row):
        reasons = []
        if row["near_certain"]:
            direction = "YES" if row["yes_price"] >= 0.90 else "NO"
            reasons.append(f"near-certain {direction} ({row['yes_price']:.0%})")
        if row["vol_liq_anomaly"]:
            reasons.append(f"vol/liq imbalance (z={row['vol_liq_zscore']:.1f})")
        if row["expiring_toss_up"]:
            reasons.append(f"expiring in {row['days_remaining']}d, still 50/50")
        return "; ".join(reasons) if reasons else ""

    df["anomaly_reason"] = df.apply(build_reason, axis=1)
    df["is_anomaly"] = df["anomaly_score"] > 0

    n_anomalies = df["is_anomaly"].sum()
    print(f"Anomaly detection complete: {n_anomalies}/{len(df)} markets flagged")
    print("\nTop anomalies:")
    print(
        df[df["is_anomaly"]]
        .sort_values("anomaly_score", ascending=False)
        [["question", "yes_price", "anomaly_score", "anomaly_reason"]]
        .head(5)
        .to_string()
    )

    return df


@test
def test_output(df) -> None:
    assert "anomaly_score" in df.columns, "Missing anomaly_score"
    assert "is_anomaly" in df.columns, "Missing is_anomaly"
    assert df["anomaly_score"].between(0, 3).all(), "anomaly_score out of expected range"
