"""Games — filterable game results and run-differential trend."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
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

st.write(f"{len(df):,} games — click a row to view boxscore")

event = st.dataframe(
    df.drop(columns=["game_pk"]),
    width="stretch",
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
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

# ── Boxscore panel ────────────────────────────────────────────────────────────
selected_rows = event.selection.rows
if selected_rows:
    game_pk = int(df.iloc[selected_rows[0]]["game_pk"])

    game = conn.execute("""
        SELECT away_team_name, away_team_abbrev, away_score,
               home_team_name, home_team_abbrev, home_score,
               game_date, venue_name, innings
        FROM gold.fact_game WHERE game_pk = ?
    """, [game_pk]).fetchone()

    if game:
        away_name, away_abbrev, away_score, home_name, home_abbrev, home_score, \
            game_date, venue_name, total_innings = game

        st.divider()
        st.subheader(
            f"{away_name} @ {home_name}  —  "
            f"{game_date.strftime('%B %-d, %Y')}  —  {venue_name}"
        )

        # Linescore
        linescore = conn.execute("""
            SELECT inning,
                   away_runs, away_hits, away_errors,
                   home_runs, home_hits, home_errors
            FROM silver.game_linescore
            WHERE game_pk = ?
            ORDER BY inning
        """, [game_pk]).df()

        if not linescore.empty:
            max_inn = max(9, int(linescore["inning"].max()))
            innings_range = range(1, max_inn + 1)

            # Build one row per team
            away_row: dict = {"Team": away_abbrev}
            home_row: dict = {"Team": home_abbrev}
            for i in innings_range:
                inn_data = linescore[linescore["inning"] == i]
                if inn_data.empty:
                    away_row[str(i)] = "-"
                    home_row[str(i)] = "-"
                else:
                    away_row[str(i)] = str(int(inn_data["away_runs"].iloc[0]))
                    home_row[str(i)] = str(int(inn_data["home_runs"].iloc[0]))

            # Totals
            away_row["R"] = str(int(linescore["away_runs"].sum()))
            away_row["H"] = str(int(linescore["away_hits"].sum()))
            away_row["E"] = str(int(linescore["away_errors"].sum()))
            home_row["R"] = str(int(linescore["home_runs"].sum()))
            home_row["H"] = str(int(linescore["home_hits"].sum()))
            home_row["E"] = str(int(linescore["home_errors"].sum()))

            ls_df = pd.DataFrame([away_row, home_row]).set_index("Team")
            st.dataframe(ls_df, width="stretch")
        else:
            st.info("Linescore not available for this game.")

        # Team totals
        boxscore = conn.execute("""
            SELECT t.team_abbrev, b.is_home,
                   b.runs, b.hits, b.errors, b.left_on_base
            FROM silver.game_boxscore b
            JOIN silver.teams t ON b.team_id = t.team_id
              AND t.season_year = (SELECT season_year FROM silver.games WHERE game_pk = ?)
            WHERE b.game_pk = ?
            ORDER BY b.is_home
        """, [game_pk, game_pk]).df()

        if not boxscore.empty:
            boxscore = boxscore.drop(columns=["is_home"])
            boxscore.columns = ["Team", "R", "H", "E", "LOB"]
            st.dataframe(boxscore, hide_index=True)

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
    st.plotly_chart(fig, width="stretch")
