"""Leaders — batting and (eventually) pitching leaderboards."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

from app import get_conn

st.set_page_config(page_title="Leaders — MLB Analytics", layout="wide")
st.title("Leaders")

# ── Constants ─────────────────────────────────────────────────────────────────

SORT_OPTIONS: dict[str, str] = {
    "HR":  "hr",
    "AVG": "avg",
    "OPS": "ops",
    "RBI": "rbi",
    "OBP": "obp",
    "SLG": "slg",
    "H":   "h",
    "R":   "r",
    "AB":  "ab",
    "2B":  "doubles",
    "3B":  "triples",
    "BB":  "bb",
    "SO":  "so",
    "G":   "g",
}

RATE_STATS = {"avg", "obp", "slg", "ops"}

GAME_TYPE_MAP: dict[str, str] = {
    "Regular Season": "('R')",
    "Postseason":     "('F','D','L','W')",
    "All":            "('R','F','D','L','W')",
}

# Exclude plate-appearance sub-types from position display/filter
NON_POSITIONS = ("PH", "PR")

POSITIONS = ["All Positions", "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH", "P"]

# ── Load filter options ────────────────────────────────────────────────────────

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

try:
    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season_year FROM silver.games ORDER BY season_year DESC"
    ).fetchall()]

    all_teams = ["All Teams"] + [r[0] for r in conn.execute(
        "SELECT DISTINCT team_abbrev FROM gold.dim_team ORDER BY team_abbrev"
    ).fetchall()]
except Exception as exc:
    conn.close()
    st.error(f"Failed to load filter options: {exc}")
    st.stop()

# ── Filter row ────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5, c6, c7 = st.columns([1, 1.4, 1, 1.4, 1.4, 1.4, 1.4])

with c1:
    season = st.selectbox("Season", seasons)
with c2:
    game_type_label = st.selectbox("Game Type", list(GAME_TYPE_MAP))
with c3:
    league = st.selectbox("League", ["MLB", "AL", "NL"])
with c4:
    team = st.selectbox("Team", all_teams)
with c5:
    position = st.selectbox("Position", POSITIONS)
with c6:
    sort_label = st.selectbox("Sort by", list(SORT_OPTIONS))
    sort_col = SORT_OPTIONS[sort_label]
with c7:
    min_ab = st.number_input("Min AB", min_value=0, value=0, step=5)

# ── Tabs ──────────────────────────────────────────────────────────────────────

hit_tab, pitch_tab = st.tabs(["Hitting", "Pitching"])

# ── Hitting tab ───────────────────────────────────────────────────────────────

with hit_tab:
    game_type_sql = GAME_TYPE_MAP[game_type_label]

    # Build optional WHERE fragments (values come from DB-sourced selectboxes, not free text)
    extra_where = []
    if team != "All Teams":
        extra_where.append(f"t.team_abbrev = '{team}'")
    if league != "MLB":
        extra_where.append(f"t.league_abbrev = '{league}'")
    extra_clause = ("AND " + " AND ".join(extra_where)) if extra_where else ""

    batting_sql = f"""
        SELECT
            p.full_name,
            MODE(gb.position_abbrev)
                FILTER (WHERE gb.position_abbrev NOT IN {NON_POSITIONS})  AS pos,
            CASE WHEN COUNT(DISTINCT t.team_abbrev) > 1 THEN '2TM'
                 ELSE ANY_VALUE(t.team_abbrev) END                        AS team,
            COUNT(DISTINCT gb.game_pk)                                    AS g,
            SUM(gb.at_bats)                                               AS ab,
            SUM(gb.runs)                                                   AS r,
            SUM(gb.hits)                                                   AS h,
            SUM(gb.doubles)                                               AS doubles,
            SUM(gb.triples)                                               AS triples,
            SUM(gb.home_runs)                                             AS hr,
            SUM(gb.rbi)                                                   AS rbi,
            SUM(gb.walks)                                                  AS bb,
            SUM(gb.strikeouts)                                            AS so,
            ROUND(SUM(gb.hits)::DOUBLE
                  / NULLIF(SUM(gb.at_bats), 0), 3)                       AS avg,
            ROUND((SUM(gb.hits) + SUM(gb.walks))::DOUBLE
                  / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0), 3)      AS obp,
            ROUND((SUM(gb.hits)
                   + SUM(gb.doubles)
                   + 2 * SUM(gb.triples)
                   + 3 * SUM(gb.home_runs))::DOUBLE
                  / NULLIF(SUM(gb.at_bats), 0), 3)                       AS slg,
            ROUND(
                (SUM(gb.hits) + SUM(gb.walks))::DOUBLE
                    / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0)
                + (SUM(gb.hits)
                   + SUM(gb.doubles)
                   + 2 * SUM(gb.triples)
                   + 3 * SUM(gb.home_runs))::DOUBLE
                    / NULLIF(SUM(gb.at_bats), 0),
                3)                                                        AS ops
        FROM silver.game_batting gb
        JOIN silver.games        sg ON gb.game_pk   = sg.game_pk
        JOIN silver.players       p ON gb.player_id  = p.player_id
        JOIN gold.dim_team        t ON gb.team_id    = t.team_id
                                   AND sg.season_year = t.season_year
        WHERE sg.season_year = {season}
          AND sg.game_type IN {game_type_sql}
          AND sg.status = 'Final'
          {extra_clause}
        GROUP BY p.player_id, p.full_name
    """

    try:
        df = conn.execute(batting_sql).df()
    except Exception as exc:
        conn.close()
        st.error(f"Query error: {exc}")
        st.stop()

    conn.close()

    # ── Client-side filters ───────────────────────────────────────────────────
    if position != "All Positions":
        df = df[df["pos"] == position]

    if min_ab > 0:
        df = df[df["ab"] >= min_ab]

    if df.empty:
        st.info("No batting data matches the current filters.")
    else:
        # Sort and dense-rank
        ascending = False  # leaders always descend (most HR, highest AVG)
        df = df.sort_values(sort_col, ascending=ascending, na_position="last")
        df.insert(0, "rank", df[sort_col].rank(method="dense", ascending=ascending, na_option="bottom").astype(int))

        if sort_col in RATE_STATS and min_ab == 0:
            st.caption("Tip: set Min AB to hide players with too few plate appearances.")

        st.dataframe(
            df[["rank", "full_name", "pos", "team",
                "g", "ab", "r", "h", "doubles", "triples", "hr", "rbi", "bb", "so",
                "avg", "obp", "slg", "ops"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "rank":    st.column_config.NumberColumn("#",      width=40),
                "full_name": st.column_config.TextColumn("Player", width=170),
                "pos":     st.column_config.TextColumn("Pos",    width=48),
                "team":    st.column_config.TextColumn("Team",   width=56),
                "g":       st.column_config.NumberColumn("G",    width=44),
                "ab":      st.column_config.NumberColumn("AB",   width=50),
                "r":       st.column_config.NumberColumn("R",    width=44),
                "h":       st.column_config.NumberColumn("H",    width=44),
                "doubles": st.column_config.NumberColumn("2B",   width=44),
                "triples": st.column_config.NumberColumn("3B",   width=44),
                "hr":      st.column_config.NumberColumn("HR",   width=44),
                "rbi":     st.column_config.NumberColumn("RBI",  width=50),
                "bb":      st.column_config.NumberColumn("BB",   width=44),
                "so":      st.column_config.NumberColumn("SO",   width=44),
                "avg":     st.column_config.NumberColumn("AVG",  format="%.3f", width=64),
                "obp":     st.column_config.NumberColumn("OBP",  format="%.3f", width=64),
                "slg":     st.column_config.NumberColumn("SLG",  format="%.3f", width=64),
                "ops":     st.column_config.NumberColumn("OPS",  format="%.3f", width=64),
            },
        )

        st.caption(f"{len(df):,} players — sorted by {sort_label}")

# ── Pitching tab ──────────────────────────────────────────────────────────────

with pitch_tab:
    st.info("Pitching leaders coming soon.")
