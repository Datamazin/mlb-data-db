"""Games — filterable game results and run-differential trend."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import plotly.express as px
import streamlit as st

from app import get_conn

st.set_page_config(page_title="Games — MLB Analytics", layout="wide")
st.title("Games")

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

# ── Filters ───────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

seasons = [r[0] for r in conn.execute(
    "SELECT DISTINCT season_year FROM gold.fact_game ORDER BY season_year DESC"
).fetchall()]

if not seasons:
    st.info("No game data yet.")
    st.stop()

with col1:
    season = st.selectbox("Season", seasons)

game_type_map = {"Regular Season": "R", "Wild Card": "F", "Division Series": "D",
                 "Championship Series": "L", "World Series": "W", "Spring Training": "S"}
with col2:
    game_type_label = st.selectbox("Game Type", ["All"] + list(game_type_map.keys()))

teams = [r[0] for r in conn.execute("""
    SELECT DISTINCT team_name FROM gold.dim_team
    WHERE season_year = ? ORDER BY team_name
""", [season]).fetchall()]

with col3:
    team_filter = st.selectbox("Team", ["All"] + teams)

# ── Build query ───────────────────────────────────────────────────────────────
where = ["season_year = ?", "status = 'Final'"]
params: list = [season]

if game_type_label != "All":
    where.append("game_type = ?")
    params.append(game_type_map[game_type_label])

if team_filter != "All":
    where.append("(home_team_name = ? OR away_team_name = ?)")
    params.extend([team_filter, team_filter])

where_clause = " AND ".join(where)

df = conn.execute(f"""
    SELECT
        game_pk,
        game_date,
        away_team_abbrev  AS away,
        away_score,
        home_score,
        home_team_abbrev  AS home,
        venue_name,
        game_type,
        innings,
        attendance,
        game_duration_min AS duration_min
    FROM gold.fact_game
    WHERE {where_clause}
    ORDER BY game_date DESC, game_pk DESC
""", params).df()

st.write(f"{len(df):,} games")
st.dataframe(
    df.drop(columns=["game_pk"]),
    use_container_width=True,
    hide_index=True,
    column_config={
        "game_date":    st.column_config.DateColumn("Date"),
        "away":         "Away",
        "away_score":   "R",
        "home_score":   "R",
        "home":         "Home",
        "venue_name":   "Venue",
        "game_type":    "Type",
        "innings":      "Inn",
        "attendance":   st.column_config.NumberColumn("Att", format="%d"),
        "duration_min": "Min",
    },
)

# ── Run-differential chart for selected team ──────────────────────────────────
if team_filter != "All" and not df.empty:
    st.divider()
    st.subheader(f"Run Differential — {team_filter} ({season})")

    trend = conn.execute("""
        SELECT
            game_date,
            game_pk,
            CASE
                WHEN home_team_name = ? THEN home_score - away_score
                ELSE away_score - home_score
            END AS run_diff
        FROM gold.fact_game
        WHERE season_year = ?
          AND status = 'Final'
          AND (home_team_name = ? OR away_team_name = ?)
        ORDER BY game_date, game_pk
    """, [team_filter, season, team_filter, team_filter]).df()

    trend["cumulative_rd"] = trend["run_diff"].cumsum()

    fig = px.line(
        trend,
        x="game_date",
        y="cumulative_rd",
        labels={"game_date": "Date", "cumulative_rd": "Cumulative Run Diff"},
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)
