import pandas as pd
from mage_ai.data_preparation.decorators import transformer, test


# ── Keyword-based categorizer (free, no API needed) ────────────────────────────
CATEGORY_RULES = {
    "Politics": [
        "president", "election", "senate", "congress", "vote", "democrat",
        "republican", "trump", "biden", "harris", "governor", "poll",
        "approve", "impeach", "legislation", "party", "minister", "parliament",
        "white house", "mayor", "campaign", "referendum", "ballot",
    ],
    "Crypto": [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "sol",
        "token", "defi", "nft", "blockchain", "coinbase", "binance",
        "altcoin", "stablecoin", "usdc", "usdt", "xrp", "doge", "matic",
        "base", "layer", "memecoin", "polymarket",
    ],
    "Economics": [
        "gdp", "inflation", "fed", "interest rate", "recession", "cpi",
        "unemployment", "market", "stock", "s&p", "nasdaq", "dow",
        "earnings", "ipo", "tariff", "trade", "economy", "fiscal",
        "monetary", "treasury", "gold", "oil", "commodit",
    ],
    "Sports": [
        "nba", "nfl", "mlb", "nhl", "football", "basketball", "baseball",
        "soccer", "ufc", "mma", "tennis", "golf", "championship", "playoff",
        "world cup", "superbowl", "super bowl", "match", "game", "win",
        "score", "team", "league", "tournament", "season", "coach",
        "transfer", "olympic", "fight",
    ],
    "Geopolitics": [
        "war", "russia", "ukraine", "china", "taiwan", "israel", "iran",
        "nato", "sanction", "military", "ceasefire", "invasion", "treaty",
        "nuclear", "conflict", "troop", "missile", "diplomacy", "un ",
        "middle east", "korea", "pakistan", "india",
    ],
    "Science & Tech": [
        "ai", "openai", "gpt", "llm", "model", "launch", "spacex", "nasa",
        "fda", "drug", "vaccine", "cancer", "climate", "energy", "tech",
        "apple", "google", "microsoft", "meta", "startup", "agi",
        "robot", "chip", "semiconductor", "quantum",
    ],
    "Entertainment": [
        "oscar", "grammy", "emmy", "award", "movie", "film", "music",
        "album", "artist", "celebrity", "tv", "show", "netflix", "box office",
        "chart", "tour", "ticket", "streaming", "release",
    ],
}


def categorize_question(question: str) -> str:
    q = question.lower()
    scores = {cat: 0 for cat in CATEGORY_RULES}
    for cat, keywords in CATEGORY_RULES.items():
        for kw in keywords:
            if kw in q:
                scores[cat] += 1
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Other"


def score_surprise(yes_price: float, volume: float, days_remaining: float) -> int:
    """
    Heuristic surprise score (1-10).
    High surprise = near-certain outcome + high stakes + close to expiry.
    """
    if pd.isnull(yes_price):
        return 5
    certainty = abs(yes_price - 0.5) * 2
    volume_factor = min(volume / 500_000, 1.0)
    urgency = 1 - min((days_remaining or 0) / 90, 1.0)
    raw = (certainty * 0.5 + volume_factor * 0.3 + urgency * 0.2) * 10
    return max(1, min(10, round(raw)))


@transformer
def enrich_markets(df, *args, **kwargs):
    """
    Free keyword-based enrichment — no API key required.
    Categorizes markets by reading question text.
    Scores surprise heuristically from price + volume + urgency.
    """
    df["llm_category"] = df["question"].apply(categorize_question)
    df["surprise_score"] = df.apply(
        lambda row: score_surprise(
            row.get("yes_price"), row.get("volume", 0), row.get("days_remaining", 30)
        ),
        axis=1,
    )
    df["llm_context"] = ""

    print("Category distribution:")
    print(df["llm_category"].value_counts().to_string())
    print(f"\nAvg surprise score: {df['surprise_score'].mean():.1f}")

    return df


@test
def test_output(df) -> None:
    assert "llm_category" in df.columns
    assert "surprise_score" in df.columns
    assert not (df["llm_category"] == "Other").all(), "All markets landed in Other"
    assert df["surprise_score"].between(1, 10).all()
