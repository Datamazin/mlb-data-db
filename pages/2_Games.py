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
col1, col2, col3, col4 = st.columns(4)

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

with col4:
    home_team_filter = st.selectbox("Home Team", ["All"] + teams)

# ── Build query ───────────────────────────────────────────────────────────────
where = ["season_year = ?", "status = 'Final'"]
params: list = [season]

if game_type_label != "All":
    where.append("game_type = ?")
    params.append(game_type_map[game_type_label])

if team_filter != "All":
    where.append("(home_team_name = ? OR away_team_name = ?)")
    params.extend([team_filter, team_filter])

if home_team_filter != "All":
    where.append("home_team_name = ?")
    params.append(home_team_filter)

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
        "attendance":   st.column_config.NumberColumn("Att", format="%,d"),
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
               game_date, venue_name, innings,
               wp_first_name, wp_last_name,
               lp_first_name, lp_last_name,
               sv_first_name, sv_last_name
        FROM gold.fact_game WHERE game_pk = ?
    """, [game_pk]).fetchone()

    if game:
        (away_name, away_abbrev, away_score, home_name, home_abbrev, home_score,
         game_date, venue_name, total_innings,
         wp_first, wp_last, lp_first, lp_last, sv_first, sv_last) = game

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

            away_r = int(linescore["away_runs"].sum())
            away_h = int(linescore["away_hits"].sum())
            away_e = int(linescore["away_errors"].sum())
            home_r = int(linescore["home_runs"].sum())
            home_h = int(linescore["home_hits"].sum())
            home_e = int(linescore["home_errors"].sum())

            def inn_cell(df: pd.DataFrame, inning: int, col: str) -> str:
                row = df[df["inning"] == inning]
                return "-" if row.empty else str(int(row[col].iloc[0]))

            css = """
<style>
.ls-wrap { overflow-x: auto; }
table.linescore {
    border-collapse: collapse;
    font-family: sans-serif;
    font-size: 14px;
    width: 100%;
}
table.linescore th, table.linescore td {
    padding: 6px 10px;
    text-align: center;
    border: 1px solid #3a3a3a;
    min-width: 30px;
}
table.linescore th {
    background: #1e1e1e;
    color: #aaa;
    font-weight: 600;
}
table.linescore td.team-name {
    text-align: left;
    font-weight: 700;
    color: #e0e0e0;
    white-space: nowrap;
    min-width: 80px;
}
table.linescore td {
    color: #d0d0d0;
    background: #141414;
}
table.linescore td.total {
    font-weight: 700;
    color: #ffffff;
    background: #1a1a2e;
    border-left: 2px solid #555;
}
table.linescore th.total {
    border-left: 2px solid #555;
    color: #fff;
}
table.linescore tr:hover td { background: #222; }
table.linescore tr:hover td.total { background: #1f1f3a; }
</style>
"""
            inn_headers = "".join(f"<th>{i}</th>" for i in innings_range)
            away_cells  = "".join(
                f"<td>{inn_cell(linescore, i, 'away_runs')}</td>" for i in innings_range
            )
            home_cells  = "".join(
                f"<td>{inn_cell(linescore, i, 'home_runs')}</td>" for i in innings_range
            )

            html = f"""
{css}
<div class="ls-wrap">
<table class="linescore">
  <thead>
    <tr>
      <th style="text-align:left">Team</th>
      {inn_headers}
      <th class="total">R</th><th class="total">H</th><th class="total">E</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td class="team-name">{away_abbrev}</td>
      {away_cells}
      <td class="total">{away_r}</td><td class="total">{away_h}</td><td class="total">{away_e}</td>
    </tr>
    <tr>
      <td class="team-name">{home_abbrev}</td>
      {home_cells}
      <td class="total">{home_r}</td><td class="total">{home_h}</td><td class="total">{home_e}</td>
    </tr>
  </tbody>
</table>
</div>
"""
            st.markdown(html, unsafe_allow_html=True)

            # W / L / S pitcher line
            decisions: list[str] = []
            if wp_last:
                decisions.append(f"**W:** {wp_first[0]}. {wp_last}" if wp_first else f"**W:** {wp_last}")
            if lp_last:
                decisions.append(f"**L:** {lp_first[0]}. {lp_last}" if lp_first else f"**L:** {lp_last}")
            if sv_last:
                decisions.append(f"**S:** {sv_first[0]}. {sv_last}" if sv_first else f"**S:** {sv_last}")
            if decisions:
                st.caption("  \u2003".join(decisions))
        else:
            st.info("Linescore not available for this game.")

        # Boxscore toggle — per-player batting stats from silver.game_batting
        show_boxscore = st.checkbox("Show Boxscore", key=f"boxscore_{game_pk}")
        if show_boxscore:
            _bat_col_cfg = {
                "Batter": st.column_config.TextColumn("Batter", width=140),
                "Pos":    st.column_config.TextColumn("Pos",    width=36),
                "AB":     st.column_config.NumberColumn("AB",   width=36),
                "R":      st.column_config.NumberColumn("R",    width=36),
                "H":      st.column_config.NumberColumn("H",    width=36),
                "2B":     st.column_config.NumberColumn("2B",   width=36),
                "3B":     st.column_config.NumberColumn("3B",   width=36),
                "HR":     st.column_config.NumberColumn("HR",   width=36),
                "RBI":    st.column_config.NumberColumn("RBI",  width=40),
                "BB":     st.column_config.NumberColumn("BB",   width=36),
                "SO":     st.column_config.NumberColumn("SO",   width=36),
                "LOB":    st.column_config.NumberColumn("LOB",  width=40),
            }

            def _fetch_batting(is_home: bool) -> pd.DataFrame:
                return conn.execute("""
                    SELECT
                        CASE WHEN gb.batting_order % 100 != 0
                             THEN '  ' || p.full_name
                             ELSE p.full_name
                        END                  AS "Batter",
                        gb.position_abbrev   AS "Pos",
                        gb.at_bats           AS "AB",
                        gb.runs              AS "R",
                        gb.hits              AS "H",
                        gb.doubles           AS "2B",
                        gb.triples           AS "3B",
                        gb.home_runs         AS "HR",
                        gb.rbi               AS "RBI",
                        gb.walks             AS "BB",
                        gb.strikeouts        AS "SO",
                        gb.left_on_base      AS "LOB"
                    FROM silver.game_batting gb
                    JOIN silver.players p ON gb.player_id = p.player_id
                    WHERE gb.game_pk = ? AND gb.is_home = ?
                    ORDER BY gb.batting_order NULLS LAST
                """, [game_pk, is_home]).df()

            away_bat = _fetch_batting(False)
            home_bat = _fetch_batting(True)

            if not away_bat.empty or not home_bat.empty:
                col_a, col_h = st.columns(2)
                with col_a:
                    st.caption(f"**{away_name}**")
                    st.dataframe(away_bat, hide_index=True, use_container_width=True,
                                 column_config=_bat_col_cfg)
                with col_h:
                    st.caption(f"**{home_name}**")
                    st.dataframe(home_bat, hide_index=True, use_container_width=True,
                                 column_config=_bat_col_cfg)
            else:
                st.info("Batting stats not available for this game.")

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
