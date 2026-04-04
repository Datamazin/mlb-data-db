"""
MLB Analytics — Streamlit frontend.
Entry point / Home page. Run with: make streamlit
"""

import os
from pathlib import Path

import duckdb
import streamlit as st

st.set_page_config(
    page_title="MLB Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path(os.getenv("MLB_DB_PATH", "data/gold/mlb.duckdb"))


@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection | None:
    if not DB_PATH.exists():
        return None
    return duckdb.connect(str(DB_PATH), read_only=True)


def main() -> None:
    st.title("MLB Analytics")
    st.caption(f"Database: {DB_PATH.resolve()}")

    conn = get_conn()
    if conn is None:
        st.error(
            f"Database not found at `{DB_PATH}`. "
            "Run `make migrate` then the pipeline to populate data."
        )
        return

    # ── Summary metrics ──────────────────────────────────────────────────────
    seasons = conn.execute(
        "SELECT COUNT(DISTINCT season_year) FROM gold.fact_game"
    ).fetchone()[0]
    total_games = conn.execute(
        "SELECT COUNT(*) FROM gold.fact_game WHERE status = 'Final'"
    ).fetchone()[0]
    total_teams = conn.execute(
        "SELECT COUNT(DISTINCT team_id) FROM gold.dim_team WHERE active"
    ).fetchone()[0]
    total_players = conn.execute(
        "SELECT COUNT(*) FROM gold.dim_player WHERE active"
    ).fetchone()[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Seasons", seasons)
    c2.metric("Games (Final)", f"{total_games:,}")
    c3.metric("Active Teams", total_teams)
    c4.metric("Active Players", f"{total_players:,}")

    st.divider()

    # ── Recent games ─────────────────────────────────────────────────────────
    st.subheader("Recent Results")

    recent = conn.execute("""
        SELECT
            game_date,
            away_team_abbrev  AS away,
            away_score,
            home_score,
            home_team_abbrev  AS home,
            venue_name,
            game_type,
            innings
        FROM gold.fact_game
        WHERE status = 'Final'
        ORDER BY game_date DESC, game_pk DESC
        LIMIT 20
    """).df()

    if recent.empty:
        st.info("No completed games found yet.")
    else:
        st.dataframe(
            recent,
            use_container_width=True,
            hide_index=True,
            column_config={
                "game_date": st.column_config.DateColumn("Date"),
                "away": "Away",
                "away_score": "R",
                "home_score": "R",
                "home": "Home",
                "venue_name": "Venue",
                "game_type": "Type",
                "innings": "Inn",
            },
        )

    # ── Season breakdown ─────────────────────────────────────────────────────
    st.subheader("Games by Season")

    by_season = conn.execute("""
        SELECT
            season_year                                         AS season,
            COUNT(*) FILTER (WHERE status = 'Final')           AS final,
            COUNT(*) FILTER (WHERE game_type = 'R')            AS regular,
            COUNT(*) FILTER (WHERE game_type IN ('F','D','L','W')) AS postseason
        FROM gold.fact_game
        GROUP BY season_year
        ORDER BY season_year
    """).df()

    if not by_season.empty:
        st.dataframe(by_season, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
