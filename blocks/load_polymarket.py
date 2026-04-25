import requests
import pandas as pd
from mage_ai.data_preparation.decorators import data_loader, test


@data_loader
def load_prediction_markets(*args, **kwargs):
    """
    Ingest active prediction markets from Polymarket's public API.
    No authentication required.
    Sorted by volume descending — highest activity markets first.
    """
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "active": "true",
        "limit": 100,
        "order": "volume",
        "ascending": "false",
        "closed": "false",
    }

    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    markets = response.json()

    df = pd.DataFrame(markets)
    print(f"Loaded {len(df)} active markets from Polymarket")
    print(f"Columns available: {list(df.columns)}")

    return df


@test
def test_output(df) -> None:
    assert df is not None, "Output is undefined"
    assert len(df) > 0, "No markets returned from Polymarket API"
    assert "question" in df.columns, "Missing 'question' column"
