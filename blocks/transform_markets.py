import json
import pandas as pd
import numpy as np
from mage_ai.data_preparation.decorators import transformer, test


@transformer
def transform_markets(df, *args, **kwargs):
    """
    Clean and normalize raw Polymarket data.
    Extracts YES probability, parses volume/liquidity,
    calculates days remaining, and adds ingestion timestamp.
    """

    def parse_yes_price(prices):
        """Outcome prices come as a JSON string e.g. '[0.72, 0.28]'"""
        try:
            if isinstance(prices, str):
                parsed = json.loads(prices)
                return float(parsed[0])
            elif isinstance(prices, list):
                return float(prices[0])
        except Exception:
            return None

    def parse_outcomes(outcomes):
        try:
            if isinstance(outcomes, str):
                parsed = json.loads(outcomes)
                return parsed[0] if parsed else "Yes"
            elif isinstance(outcomes, list):
                return outcomes[0]
        except Exception:
            return "Yes"

    # Core probability columns
    df["yes_price"] = df["outcomePrices"].apply(parse_yes_price)
    df["no_price"] = df["yes_price"].apply(lambda x: round(1 - x, 4) if pd.notnull(x) else None)
    df["yes_label"] = df["outcomes"].apply(parse_outcomes)

    # Volume and liquidity as floats
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).round(2)
    df["liquidity"] = pd.to_numeric(df.get("liquidity", 0), errors="coerce").fillna(0).round(2)

    # Time to resolution
    df["endDate"] = pd.to_datetime(df.get("endDate"), errors="coerce", utc=True)
    now = pd.Timestamp.now(tz="UTC")
    df["days_remaining"] = (df["endDate"] - now).dt.days.clip(lower=0)

    # Market certainty — how far from 50/50
    df["certainty_score"] = df["yes_price"].apply(
        lambda x: round(abs(x - 0.5) * 2, 4) if pd.notnull(x) else None
    )  # 0 = perfectly uncertain, 1 = fully resolved

    # Keep relevant columns only
    keep = [
        "id", "question", "yes_price", "no_price", "yes_label",
        "volume", "liquidity", "days_remaining", "certainty_score",
        "endDate", "active",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Drop rows with no usable price
    df = df.dropna(subset=["yes_price"])

    df["ingested_at"] = pd.Timestamp.now()

    print(f"Transformed {len(df)} markets")
    print(df[["question", "yes_price", "volume", "days_remaining"]].head(5).to_string())

    return df


@test
def test_output(df) -> None:
    assert "yes_price" in df.columns, "Missing yes_price"
    assert df["yes_price"].between(0, 1).all(), "yes_price out of [0,1] range"
    assert "volume" in df.columns, "Missing volume"
    assert len(df) > 0, "Empty dataframe after transform"
