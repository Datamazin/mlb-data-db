"""Standings — division standings from gold.standings_snap."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import plotly.express as px
import streamlit as st

from app import get_conn

st.set_page_config(page_title="Standings — MLB Analytics", layout="wide")
st.title("Standings")

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

# ── Filters ───────────────────────────────────────────────────────────────────
seasons = conn.execute(
    "SELECT DISTINCT season_year FROM gold.standings_snap ORDER BY season_year DESC"
).fetchall()

if not seasons:
    st.info("No standings data yet. Run the aggregation pipeline to populate.")
    st.stop()

season_options = [r[0] for r in seasons]
selected_season = st.selectbox("Season", season_options)

# ── Fetch standings ───────────────────────────────────────────────────────────
df = conn.execute("""
    SELECT
        s.team_id,
        t.team_name,
        t.team_abbrev,
        t.division_name,
        t.league_name,
        s.wins,
        s.losses,
        s.win_pct,
        s.games_back,
        s.streak,
        s.last_10_wins || '-' || s.last_10_losses AS last_10,
        s.home_wins  || '-' || s.home_losses      AS home,
        s.away_wins  || '-' || s.away_losses      AS away,
        s.run_diff
    FROM gold.standings_snap s
    JOIN gold.dim_team t ON s.team_id = t.team_id AND s.season_year = t.season_year
    WHERE s.season_year = ?
      AND s.snap_date = (
          SELECT MAX(snap_date) FROM gold.standings_snap WHERE season_year = ?
      )
    ORDER BY t.league_name, t.division_name, s.wins DESC
""", [selected_season, selected_season]).df()

if df.empty:
    st.info("No standings rows for this season.")
    st.stop()

# ── Display by division ───────────────────────────────────────────────────────
for division in sorted(df["division_name"].unique()):
    div_df = df[df["division_name"] == division].copy()
    league = div_df["league_name"].iloc[0]
    st.subheader(f"{league} — {division}")

    display = div_df[["team_name", "wins", "losses", "win_pct", "games_back",
                       "streak", "last_10", "home", "away", "run_diff"]].copy()
    display.columns = ["Team", "W", "L", "PCT", "GB", "Streak", "L10", "Home", "Away", "RD"]

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "PCT": st.column_config.NumberColumn(format="%.3f"),
            "GB":  st.column_config.NumberColumn(format="%.1f"),
        },
    )

# ── Win % bar chart ───────────────────────────────────────────────────────────
st.divider()
st.subheader("Win % by Team")

chart_df = df.sort_values("win_pct", ascending=True)
fig = px.bar(
    chart_df,
    x="win_pct",
    y="team_abbrev",
    color="division_name",
    orientation="h",
    labels={"win_pct": "Win %", "team_abbrev": "Team", "division_name": "Division"},
    text_auto=".3f",
)
fig.update_layout(height=600, yaxis_title=None, showlegend=True)
st.plotly_chart(fig, use_container_width=True)
