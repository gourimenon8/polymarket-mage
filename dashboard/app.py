import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

DB_PATH = "prediction_markets.db"

st.set_page_config(
    page_title="Prediction Market Intelligence",
    page_icon="🔮",
    layout="wide",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
.explainer-box {
    background: #1a1a2e;
    border-left: 4px solid #7c3aed;
    padding: 1rem 1.5rem;
    border-radius: 0 8px 8px 0;
    margin-bottom: 1.5rem;
}
.pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-right: 4px;
}
.pill-red   { background:#fca5a5; color:#7f1d1d; }
.pill-amber { background:#fcd34d; color:#78350f; }
.pill-green { background:#6ee7b7; color:#064e3b; }
.anomaly-card {
    background: #1e1e2e;
    border: 1px solid #374151;
    border-radius: 10px;
    padding: 1rem 1.2rem;
    margin-bottom: 0.75rem;
}
</style>
""", unsafe_allow_html=True)


# ── Load data ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM markets", conn)
        conn.close()
        for col in ["yes_price", "volume", "liquidity", "days_remaining",
                    "surprise_score", "certainty_score", "anomaly_score"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        return pd.DataFrame(), str(e)


result = load_data()
df = result if isinstance(result, pd.DataFrame) else result[0]

# ── Hero header ────────────────────────────────────────────────────────────────
st.title("🔮 Prediction Market Intelligence")
st.markdown(
    "**Live signals from [Polymarket](https://polymarket.com) — "
    "the world's largest prediction market platform.** "
    "Built with [Mage AI](https://mage.ai) · Refreshes every 5 min."
)

# ── What is this? explainer ────────────────────────────────────────────────────
with st.expander("📖 What is this? How does it work?", expanded=False):
    st.markdown("""
**Prediction markets** are real-money markets where people bet on the outcome of future events — elections, crypto prices, sports, geopolitics. The market price = the crowd's probability estimate. For example, a YES price of **0.72** means the market believes there's a **72% chance** the event happens.

**What this pipeline does:**

| Step | What happens |
|---|---|
| **Ingest** | Pulls 100 active markets from Polymarket's public API every run |
| **Transform** | Parses probabilities, trading volume, liquidity, and days until resolution |
| **Detect** | Flags markets that are behaving unusually (see Anomaly tab) |
| **Categorize** | Classifies each market into Politics, Crypto, Sports, Economics, etc. |
| **Store** | Saves a snapshot to SQLite, appends to historical log |
| **Visualize** | This dashboard |

**Built with [Mage AI](https://mage.ai)** — an open-source data pipeline tool with a block-based architecture. Each step above is an independent, testable Mage block.

---
**Three anomaly signals we track:**
- 🔴 **Near-certain** — market is ≥90% YES or ≤10% YES (approaching resolution)
- 🟡 **Vol/Liq imbalance** — unusually high trading volume relative to available liquidity (z-score > 2σ)
- 🟠 **Expiring toss-up** — resolves within 7 days but still 35–65% (outcome genuinely unknown)
    """)

if df.empty:
    st.warning("⚠️ No data found. Run `python seed_db.py` first to populate the database.")
    st.code("python seed_db.py", language="bash")
    st.stop()

ingested_at = df["ingested_at"].max() if "ingested_at" in df.columns else "—"
st.caption(f"🕐 Last updated: {ingested_at}")
st.divider()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🎛️ Filters")
    st.caption("Narrow down the markets you want to explore.")

    categories = sorted(df["llm_category"].dropna().unique())
    selected_cats = st.multiselect(
        "Category", categories, default=categories,
        help="Filter markets by topic category"
    )
    show_anomalies_only = st.checkbox(
        "🚨 Show anomalies only", value=False,
        help="Only show markets flagged by the anomaly detector"
    )
    min_volume = st.slider(
        "Min Trading Volume ($)", 0, int(df["volume"].max()), 0, step=5000,
        help="Filter out low-activity markets"
    )

    st.divider()
    st.markdown("**About**")
    st.caption(
        "Pipeline built by [Gouri Menon](https://www.linkedin.com/in/gouri-menon-646b17b1/) "
        "using Mage AI + Polymarket API. "
        "Data refreshes on each pipeline run."
    )

filtered = df[df["llm_category"].isin(selected_cats) & (df["volume"] >= min_volume)]
if show_anomalies_only:
    filtered = filtered[filtered["is_anomaly"] == 1]

# ── KPI row ────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric(
    "📊 Active Markets", len(filtered),
    help="Number of active Polymarket markets loaded"
)
k2.metric(
    "🚨 Anomalies Flagged", int(filtered["is_anomaly"].sum()),
    help="Markets with at least one anomaly signal"
)
k3.metric(
    "💰 Total Volume", f"${filtered['volume'].sum()/1_000_000:.1f}M",
    help="Total USD traded across visible markets"
)
k4.metric(
    "⚖️ Avg Probability", f"{filtered['yes_price'].mean():.1%}",
    help="Average YES probability across markets"
)
k5.metric(
    "🎯 Avg Certainty", f"{filtered['certainty_score'].mean():.1%}",
    help="How far markets are from 50/50 on average (0% = all uncertain, 100% = all resolved)"
)

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Market Overview",
    "🚨 Anomaly Detector",
    "📈 Distributions",
    "🗄️ Raw Data",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Market Overview
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("🏆 Top Markets by Trading Volume")
        st.caption("The most actively traded markets right now — high volume = high market interest.")

        top30 = filtered.sort_values("volume", ascending=False).head(30)
        display = top30[["question", "yes_price", "volume", "days_remaining",
                          "llm_category", "anomaly_score"]].copy()

        # Color-code anomaly score
        def fmt_anomaly(score):
            if score == 0: return "✅ Clean"
            if score == 1: return "🟡 Watch"
            return "🔴 Flagged"

        display["anomaly_score"] = display["anomaly_score"].apply(fmt_anomaly)
        display["yes_price"] = display["yes_price"].map("{:.1%}".format)
        display["volume"] = display["volume"].map("${:,.0f}".format)
        display["days_remaining"] = display["days_remaining"].apply(
            lambda x: f"{int(x)}d" if pd.notnull(x) else "—"
        )
        display.columns = ["Question", "YES%", "Volume", "Expires", "Category", "Status"]
        st.dataframe(display, use_container_width=True, hide_index=True)

    with col_right:
        st.subheader("📂 Volume by Category")
        st.caption("Where the money is concentrated.")
        cat_vol = filtered.groupby("llm_category")["volume"].sum().reset_index()
        cat_vol = cat_vol.sort_values("volume", ascending=True)
        fig = px.bar(
            cat_vol, x="volume", y="llm_category", orientation="h",
            labels={"volume": "Total Volume ($)", "llm_category": ""},
            color="volume", color_continuous_scale="purples",
        )
        fig.update_layout(
            coloraxis_showscale=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=0, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("🎯 Market Count by Category")
        cat_count = filtered["llm_category"].value_counts().reset_index()
        cat_count.columns = ["Category", "Count"]
        fig2 = px.pie(
            cat_count, names="Category", values="Count",
            color_discrete_sequence=px.colors.sequential.Purples_r,
            hole=0.4,
        )
        fig2.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(font=dict(size=11))
        )
        st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Anomaly Detector
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("🚨 Anomaly Detector")
    st.markdown("""
    The pipeline runs **three independent statistical checks** on every market.
    Markets that trip multiple flags are worth watching closely.

    | Signal | What it means |
    |---|---|
    | 🔴 **Near-certain** | Probability ≥90% or ≤10% — market is approaching resolution |
    | 🟡 **Vol/Liq imbalance** | Trading volume is a 2σ outlier relative to available liquidity |
    | 🟠 **Expiring toss-up** | Resolves within 7 days but outcome is genuinely 50/50 |
    """)

    st.divider()

    anomalies = filtered[filtered["is_anomaly"] == 1].sort_values("anomaly_score", ascending=False)

    if anomalies.empty:
        st.info("No anomalies in the current filtered view.")
    else:
        st.caption(f"**{len(anomalies)} markets flagged** — sorted by severity")

        for _, row in anomalies.iterrows():
            severity = int(row["anomaly_score"])
            icon = "🔴" if severity >= 2 else "🟡"

            with st.expander(f"{icon} [{severity}/3] {row['question'][:95]}"):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("YES Probability", f"{row['yes_price']:.1%}")
                c2.metric("Volume", f"${row['volume']:,.0f}")
                c3.metric("Days Left", f"{int(row['days_remaining'])}d" if pd.notnull(row['days_remaining']) else "—")
                c4.metric("Category", row.get("llm_category", "—"))

                st.markdown(f"**🔍 Why flagged:** {row['anomaly_reason']}")

                # Visual probability bar
                yes_p = float(row["yes_price"])
                st.markdown("**Probability gauge:**")
                prob_bar = go.Figure(go.Bar(
                    x=[yes_p], orientation="h",
                    marker_color="#7c3aed",
                    text=[f"YES {yes_p:.0%}"],
                    textposition="inside",
                ))
                prob_bar.add_shape(type="line", x0=0.5, x1=0.5, y0=-0.5, y1=0.5,
                                   line=dict(color="white", dash="dot", width=2))
                prob_bar.update_layout(
                    xaxis=dict(range=[0,1], tickformat=".0%"),
                    yaxis=dict(visible=False),
                    height=80, margin=dict(l=0,r=0,t=0,b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                )
                st.plotly_chart(prob_bar, use_container_width=True)

    # Anomaly breakdown chart
    st.divider()
    st.subheader("Anomaly Signal Breakdown")
    st.caption("How many markets triggered each type of anomaly signal.")
    signal_counts = {
        "Near-certain outcome": int(filtered["near_certain"].sum()),
        "Vol/Liq imbalance": int(filtered["vol_liq_anomaly"].sum()),
        "Expiring toss-up": int(filtered["expiring_toss_up"].sum()),
    }
    sig_df = pd.DataFrame(list(signal_counts.items()), columns=["Signal", "Count"])
    fig3 = px.bar(sig_df, x="Signal", y="Count",
                  color="Count", color_continuous_scale="reds",
                  text="Count")
    fig3.update_layout(coloraxis_showscale=False,
                       plot_bgcolor="rgba(0,0,0,0)",
                       paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Distributions
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("📈 Market Distributions")
    st.caption("Understanding the shape of the prediction market landscape.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**YES Probability Distribution**")
        st.caption("A spike near 0% or 100% = many markets near resolution. A spike at 50% = many genuinely uncertain markets.")
        fig4 = px.histogram(filtered, x="yes_price", nbins=20,
                            labels={"yes_price": "YES Probability"},
                            color_discrete_sequence=["#7c3aed"])
        fig4.add_vline(x=0.5, line_dash="dot", line_color="white",
                       annotation_text="50/50", annotation_position="top")
        fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig4, use_container_width=True)

    with col2:
        st.markdown("**Days Until Resolution**")
        st.caption("Short-dated markets (< 7 days) that are still uncertain are the most interesting anomalies.")
        fig5 = px.histogram(filtered, x="days_remaining", nbins=20,
                            labels={"days_remaining": "Days Remaining"},
                            color_discrete_sequence=["#06b6d4"])
        fig5.add_vline(x=7, line_dash="dot", line_color="orange",
                       annotation_text="7-day threshold", annotation_position="top")
        fig5.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig5, use_container_width=True)

    st.markdown("**Volume vs. Certainty — where is the market most confident and most active?**")
    st.caption("Top-right = high certainty + high volume = markets about to resolve. Top-left = high uncertainty + high volume = contested markets.")
    fig6 = px.scatter(
        filtered, x="certainty_score", y="volume",
        color="llm_category", size="anomaly_score",
        size_max=20,
        hover_data=["question", "yes_price", "days_remaining"],
        labels={"certainty_score": "Certainty (0=uncertain, 1=certain)",
                "volume": "Trading Volume ($)"},
    )
    fig6.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig6, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Raw Data
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("🗄️ Full Dataset")
    st.caption(f"{len(filtered)} markets · Click any column header to sort")

    raw = filtered.copy()
    raw["yes_price"] = raw["yes_price"].map("{:.1%}".format)
    raw["volume"] = raw["volume"].map("${:,.0f}".format)
    raw["is_anomaly"] = raw["is_anomaly"].apply(lambda x: "🚨 Yes" if x else "✅ No")
    st.dataframe(raw, use_container_width=True, hide_index=True)

    csv = filtered.to_csv(index=False)
    st.download_button(
        "⬇️ Download CSV", csv, "polymarket_markets.csv", "text/csv"
    )
