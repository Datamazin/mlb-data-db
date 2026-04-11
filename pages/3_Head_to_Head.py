"""Head to Head — season win/loss records between any two teams."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import plotly.graph_objects as go
import streamlit as st

from app import get_conn

st.set_page_config(page_title="Head to Head — MLB Analytics", layout="wide")
st.title("Head to Head")

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

# ── Team selectors ────────────────────────────────────────────────────────────
teams = conn.execute("""
    SELECT DISTINCT team_name, team_abbrev
    FROM gold.dim_team
    ORDER BY team_name
""").df()

if teams.empty:
    conn.close()
    st.info("No team data yet.")
    st.stop()

team_names = teams["team_name"].tolist()

col1, col2 = st.columns(2)
with col1:
    team_a = st.selectbox("Team A", team_names, index=0)
with col2:
    remaining = [t for t in team_names if t != team_a]
    team_b = st.selectbox("Team B", remaining, index=0)

seasons = [r[0] for r in conn.execute(
    "SELECT DISTINCT season_year FROM gold.head_to_head ORDER BY season_year DESC"
).fetchall()]

season_choice = st.selectbox("Season", ["All"] + [str(s) for s in seasons])

# ── Fetch IDs ─────────────────────────────────────────────────────────────────
id_a = conn.execute(
    "SELECT team_id FROM gold.dim_team WHERE team_name = ? LIMIT 1", [team_a]
).fetchone()
id_b = conn.execute(
    "SELECT team_id FROM gold.dim_team WHERE team_name = ? LIMIT 1", [team_b]
).fetchone()

if not id_a or not id_b:
    conn.close()
    st.warning("Could not resolve team IDs.")
    st.stop()

id_a, id_b = id_a[0], id_b[0]

# ── Fetch matchup record ──────────────────────────────────────────────────────
season_filter = "" if season_choice == "All" else f"AND season_year = {int(season_choice)}"

record = conn.execute(f"""
    SELECT
        SUM(wins)         AS wins,
        SUM(losses)       AS losses,
        SUM(games_played) AS gp
    FROM gold.head_to_head
    WHERE team_id = ? AND opponent_id = ?
    {season_filter}
""", [id_a, id_b]).fetchone()

wins_a, losses_a, gp = (record[0] or 0), (record[1] or 0), (record[2] or 0)
wins_b = losses_a  # A's losses = B's wins in this matchup

# ── Display ───────────────────────────────────────────────────────────────────
st.subheader(f"{team_a} vs {team_b}" + (f" ({season_choice})" if season_choice != "All" else " (All Seasons)"))

if gp == 0:
    st.info("No head-to-head games found for this selection.")
else:
    m1, m2, m3 = st.columns(3)
    m1.metric(f"{team_a} Wins", int(wins_a))
    m2.metric("Games Played", int(gp))
    m3.metric(f"{team_b} Wins", int(wins_b))

    # Donut chart
    abbrev_a = teams.loc[teams["team_name"] == team_a, "team_abbrev"].iloc[0]
    abbrev_b = teams.loc[teams["team_name"] == team_b, "team_abbrev"].iloc[0]

    fig = go.Figure(go.Pie(
        labels=[abbrev_a, abbrev_b],
        values=[wins_a, wins_b],
        hole=0.5,
        textinfo="label+percent",
    ))
    fig.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig, width="stretch")

# ── Game log ──────────────────────────────────────────────────────────────────
st.subheader("Game Log")

log = conn.execute(f"""
    SELECT
        g.game_date,
        g.season_year AS season,
        g.away_team_abbrev AS away,
        g.away_score,
        g.home_score,
        g.home_team_abbrev AS home,
        g.venue_name,
        g.innings
    FROM gold.fact_game g
    WHERE g.status = 'Final'
      AND g.game_type = 'R'
      AND ((g.home_team_id = ? AND g.away_team_id = ?)
        OR (g.home_team_id = ? AND g.away_team_id = ?))
    {season_filter.replace('season_year', 'g.season_year')}
    ORDER BY g.game_date DESC
""", [id_a, id_b, id_b, id_a]).df()
conn.close()

if log.empty:
    st.info("No game log entries.")
else:
    st.dataframe(log, width="stretch", hide_index=True,
                 column_config={"game_date": st.column_config.DateColumn("Date")})
