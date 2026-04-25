# 🔮 Prediction Market Intelligence Pipeline

> A production-style data pipeline built with **Mage AI** that ingests live prediction market data from Polymarket, detects anomalies, categorizes markets, and surfaces insights on a live Streamlit dashboard.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Mage AI](https://img.shields.io/badge/Built%20with-Mage%20AI-purple)
![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-red)
![Data](https://img.shields.io/badge/Source-Polymarket-green)

---

## What It Does

Prediction markets are real-time uncertainty quantification at scale, where every price is a crowd-sourced probability estimate backed by real money. This pipeline treats them as a structured data source and surfaces signals that matter.

| Stage | Block | Description |
|---|---|---|
| **Ingest** | `load_polymarket.py` | Pulls 100 active markets from Polymarket's free public API |
| **Transform** | `transform_markets.py` | Parses probabilities, volume, liquidity, days remaining, certainty score |
| **Detect** | `detect_anomalies.py` | Flags vol/liquidity imbalances, near-certain outcomes, expiring toss-ups |
| **Enrich** | `llm_enrichment.py` | Keyword-based categorization + heuristic surprise scoring |
| **Export** | `export_to_sqlite.py` | Persists to SQLite — latest snapshot + append-only history |
| **Visualize** | `dashboard/app.py` | Streamlit UI with 4 tabs: overview, anomalies, insights, distributions |

---

## Pipeline Architecture

```
Polymarket API
      │
      ▼
[load_polymarket]        ← Data Loader
      │
      ▼
[transform_markets]      ← Transformer: parse prices, volume, time
      │
      ▼
[detect_anomalies]       ← Transformer: z-score flagging
      │
      ▼
[llm_enrichment]         ← Transformer: categorize + surprise score
      │
      ▼
[export_to_sqlite]       ← Data Exporter: SQLite (snapshot + history)
      │
      ▼
[Streamlit Dashboard]    ← Live UI
```

---

## Anomaly Detection Logic

Three independent flags, composited into an anomaly score (0–3):

| Flag | Logic |
|---|---|
| `near_certain` | YES price ≥ 90% or ≤ 10% — market approaching resolution |
| `vol_liq_anomaly` | Volume/liquidity z-score > 2σ — thin liquidity, high activity |
| `expiring_toss_up` | Resolves within 7 days, still between 35–65% — unresolved uncertainty |

---

## Setup & Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start Mage
```bash
mage start polymarket_pipeline
```

### 3. Create the pipeline in Mage UI
1. Click **New Pipeline** → Standard (Batch)
2. Add blocks in order from `blocks/` folder
3. Connect each block to the next in the graph
4. Click **Run Pipeline**

### 4. Launch the dashboard
```bash
https://polymarket-mage8.streamlit.app/
```

---

## Project Structure

```
polymarket_mage/
├── README.md
├── requirements.txt
├── blocks/
│   ├── load_polymarket.py
│   ├── transform_markets.py
│   ├── detect_anomalies.py
│   ├── llm_enrichment.py
│   └── export_to_sqlite.py
└── dashboard/
    └── app.py
```

---

## Why Mage?

Mage's block-based architecture made it easy to develop and iterate every transformation step independently. Each block is testable in isolation, failed steps don't require full re-runs, and new enrichment layers plug in without touching upstream logic, which is exactly the kind of modularity that makes production pipelines maintainable.

---

## Author

**Gouri Menon** — Data Scientist | Associate II, Columbia University  
[LinkedIn](https://www.linkedin.com/in/gouri-menon-646b17b1/)
