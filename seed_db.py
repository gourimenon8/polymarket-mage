"""
seed_db.py — Run this ONCE to populate prediction_markets.db
without needing Mage running. Used for Streamlit Cloud deployment.

Usage:
    python seed_db.py
"""

import json
import re
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timezone

DB_PATH = "prediction_markets.db"

CATEGORY_RULES = {
    "Politics": ["president","election","senate","congress","vote","democrat","republican",
        "trump","biden","harris","governor","poll","approve","impeach","legislation",
        "party","minister","parliament","white house","mayor","campaign","referendum","ballot"],
    "Crypto": ["bitcoin","btc","ethereum","eth","crypto","solana","sol","token","defi",
        "nft","blockchain","coinbase","binance","altcoin","stablecoin","usdc","usdt",
        "xrp","doge","matic","base","layer","memecoin","polymarket"],
    "Economics": ["gdp","inflation","fed","interest rate","recession","cpi","unemployment",
        "market","stock","s&p","nasdaq","dow","earnings","ipo","tariff","trade",
        "economy","fiscal","monetary","treasury","gold","oil","commodit"],
    "Sports": ["nba","nfl","mlb","nhl","football","basketball","baseball","soccer","ufc",
        "mma","tennis","golf","championship","playoff","world cup","superbowl","super bowl",
        "match","game","win","score","team","league","tournament","season","coach",
        "transfer","olympic","fight"],
    "Geopolitics": ["war","russia","ukraine","china","taiwan","israel","iran","nato",
        "sanction","military","ceasefire","invasion","treaty","nuclear","conflict",
        "troop","missile","diplomacy","un ","middle east","korea","pakistan","india"],
    "Science & Tech": ["ai","openai","gpt","llm","model","launch","spacex","nasa","fda",
        "drug","vaccine","cancer","climate","energy","tech","apple","google","microsoft",
        "meta","startup","agi","robot","chip","semiconductor","quantum"],
    "Entertainment": ["oscar","grammy","emmy","award","movie","film","music","album",
        "artist","celebrity","tv","show","netflix","box office","chart","tour","ticket",
        "streaming","release"],
}

def categorize(question):
    q = question.lower()
    scores = {cat: sum(1 for kw in kws if kw in q) for cat, kws in CATEGORY_RULES.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "Other"

def surprise_score(yes_price, volume, days_remaining):
    if pd.isnull(yes_price): return 5
    certainty = abs(yes_price - 0.5) * 2
    vol_factor = min(volume / 500_000, 1.0)
    urgency = 1 - min((days_remaining or 0) / 90, 1.0)
    return max(1, min(10, round((certainty * 0.5 + vol_factor * 0.3 + urgency * 0.2) * 10)))

def parse_yes_price(prices):
    try:
        parsed = json.loads(prices) if isinstance(prices, str) else prices
        return float(parsed[0])
    except: return None

def run():
    print("Fetching markets from Polymarket...")
    r = requests.get("https://gamma-api.polymarket.com/markets",
        params={"active":"true","limit":100,"order":"volume","ascending":"false","closed":"false"},
        timeout=15)
    r.raise_for_status()
    df = pd.DataFrame(r.json())

    df["yes_price"] = df["outcomePrices"].apply(parse_yes_price)
    df["no_price"] = df["yes_price"].apply(lambda x: round(1-x,4) if pd.notnull(x) else None)
    df["volume"] = pd.to_numeric(df.get("volume",0), errors="coerce").fillna(0).round(2)
    df["liquidity"] = pd.to_numeric(df.get("liquidity",0), errors="coerce").fillna(0).round(2)
    df["endDate"] = pd.to_datetime(df.get("endDate"), errors="coerce", utc=True)
    now = pd.Timestamp.now(tz="UTC")
    df["days_remaining"] = (df["endDate"] - now).dt.days.clip(lower=0).fillna(0).astype(int)
    df["certainty_score"] = df["yes_price"].apply(lambda x: round(abs(x-0.5)*2,4) if pd.notnull(x) else None)
    df = df.dropna(subset=["yes_price"])

    # Anomaly detection
    df["near_certain"] = df["yes_price"].apply(lambda x: int(x>=0.90 or x<=0.10))
    df["vol_liq_ratio"] = df["volume"] / (df["liquidity"] + 1.0)
    mean, std = df["vol_liq_ratio"].mean(), df["vol_liq_ratio"].std() + 1e-9
    df["vol_liq_zscore"] = (df["vol_liq_ratio"] - mean) / std
    df["vol_liq_anomaly"] = (df["vol_liq_zscore"] > 2.0).astype(int)
    df["expiring_toss_up"] = ((df["days_remaining"]<=7) & df["yes_price"].between(0.35,0.65)).astype(int)
    df["anomaly_score"] = df["near_certain"] + df["vol_liq_anomaly"] + df["expiring_toss_up"]
    df["is_anomaly"] = (df["anomaly_score"] > 0).astype(int)

    def reason(row):
        r = []
        if row["near_certain"]: r.append(f"near-certain ({row['yes_price']:.0%})")
        if row["vol_liq_anomaly"]: r.append(f"vol/liq imbalance (z={row['vol_liq_zscore']:.1f})")
        if row["expiring_toss_up"]: r.append(f"expiring in {row['days_remaining']}d, 50/50")
        return "; ".join(r)
    df["anomaly_reason"] = df.apply(reason, axis=1)

    # Enrichment
    df["llm_category"] = df["question"].apply(categorize)
    df["surprise_score"] = df.apply(lambda row: surprise_score(row["yes_price"], row["volume"], row["days_remaining"]), axis=1)
    df["llm_context"] = ""
    df["ingested_at"] = pd.Timestamp.now()

    # Export
    keep = ["id","question","yes_price","no_price","volume","liquidity","days_remaining",
            "certainty_score","near_certain","vol_liq_anomaly","expiring_toss_up",
            "anomaly_score","is_anomaly","anomaly_reason","llm_category","surprise_score",
            "llm_context","ingested_at"]
    out = df[[c for c in keep if c in df.columns]]

    conn = sqlite3.connect(DB_PATH)
    out.to_sql("markets", conn, if_exists="replace", index=False)
    out.to_sql("markets_history", conn, if_exists="append", index=False)
    conn.close()

    print(f"Done. {len(out)} markets written to {DB_PATH}")
    print(out["llm_category"].value_counts().to_string())

if __name__ == "__main__":
    run()
