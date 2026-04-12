"""Leaders — batting and pitching leaderboards."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

from app import get_conn

st.set_page_config(page_title="Leaders — MLB Analytics", layout="wide")
st.title("Leaders")

# ── Constants ─────────────────────────────────────────────────────────────────

GAME_TYPE_MAP: dict[str, str] = {
    "Regular Season": "('R')",
    "Postseason":     "('F','D','L','W')",
    "All":            "('R','F','D','L','W')",
}

BAT_SORT: dict[str, tuple[str, bool]] = {
    # label → (column, descending)
    "HR":  ("hr",      True),
    "AVG": ("avg",     True),
    "OPS": ("ops",     True),
    "RBI": ("rbi",     True),
    "OBP": ("obp",     True),
    "SLG": ("slg",     True),
    "H":   ("h",       True),
    "R":   ("r",       True),
    "AB":  ("ab",      True),
    "2B":  ("doubles", True),
    "3B":  ("triples", True),
    "BB":  ("bb",      True),
    "SO":  ("so",      True),
    "G":   ("g",       True),
}

PIT_SORT: dict[str, tuple[str, bool]] = {
    # label → (column, descending)  False = ascending (lower is better)
    "ERA":  ("era",  False),
    "SO":   ("so",   True),
    "WHIP": ("whip", False),
    "W":    ("w",    True),
    "SV":   ("sv",   True),
    "IP":   ("outs", True),   # sort by raw outs for correct ordering
    "BB":   ("bb",   False),
    "HR":   ("hr",   False),
    "HLD":  ("hld",  True),
    "G":    ("g",    True),
    "GS":   ("gs",   True),
}

BAT_POSITIONS = ["All Positions", "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH", "P"]
NON_POSITIONS  = ("PH", "PR")

# ── Load seasons ──────────────────────────────────────────────────────────────

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

try:
    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season_year FROM silver.games ORDER BY season_year DESC"
    ).fetchall()]
except Exception as exc:
    conn.close()
    st.error(f"Failed to load seasons: {exc}")
    st.stop()

# ── Shared filter row ─────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns([1, 1.5, 1, 1.5])

with c1:
    season = st.selectbox("Season", seasons)

try:
    all_teams = ["All Teams"] + [r[0] for r in conn.execute(
        "SELECT DISTINCT team_abbrev FROM gold.dim_team WHERE season_year = ? ORDER BY team_abbrev",
        [season],
    ).fetchall()]
except Exception as exc:
    conn.close()
    st.error(f"Failed to load teams: {exc}")
    st.stop()

with c2:
    game_type_label = st.selectbox("Game Type", list(GAME_TYPE_MAP))
with c3:
    league = st.selectbox("League", ["MLB", "AL", "NL"])
with c4:
    team = st.selectbox("Team", all_teams)

# ── Build shared WHERE fragment ───────────────────────────────────────────────

game_type_sql = GAME_TYPE_MAP[game_type_label]
extra_where: list[str] = []
if team != "All Teams":
    extra_where.append(f"t.team_abbrev = '{team}'")
if league != "MLB":
    extra_where.append(f"t.league_abbrev = '{league}'")
shared_extra = ("AND " + " AND ".join(extra_where)) if extra_where else ""

# ── Tabs ──────────────────────────────────────────────────────────────────────

hit_tab, pitch_tab = st.tabs(["Hitting", "Pitching"])

# ═══════════════════════════════════════════════════════════════════════════════
# HITTING TAB
# ═══════════════════════════════════════════════════════════════════════════════

with hit_tab:
    hc1, hc2, hc3 = st.columns([1.5, 1.5, 1])
    with hc1:
        position = st.selectbox("Position", BAT_POSITIONS)
    with hc2:
        bat_sort_label = st.selectbox("Sort by", list(BAT_SORT), key="bat_sort")
    with hc3:
        bat_min_ab = st.number_input("Min AB", min_value=0, value=0, step=5, key="bat_min_ab")

    bat_col, bat_desc = BAT_SORT[bat_sort_label]

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
          {shared_extra}
        GROUP BY p.player_id, p.full_name
    """

    try:
        df_bat = conn.execute(batting_sql).df()
    except Exception as exc:
        conn.close()
        st.error(f"Batting query error: {exc}")
        st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# PITCHING TAB
# ═══════════════════════════════════════════════════════════════════════════════

with pitch_tab:
    pc1, pc2, pc3 = st.columns([1.5, 1.5, 1])
    with pc1:
        role = st.selectbox("Role", ["All", "Starters", "Relievers"])
    with pc2:
        pit_sort_label = st.selectbox("Sort by", list(PIT_SORT), key="pit_sort")
    with pc3:
        pit_min_ip = st.number_input("Min IP", min_value=0, value=0, step=5, key="pit_min_ip")

    pit_col, pit_desc = PIT_SORT[pit_sort_label]

    role_filter = ""
    if role == "Starters":
        role_filter = "HAVING SUM(gp.games_started) > 0"
    elif role == "Relievers":
        role_filter = "HAVING SUM(gp.games_started) = 0"

    pitching_sql = f"""
        SELECT
            p.full_name,
            CASE WHEN COUNT(DISTINCT t.team_abbrev) > 1 THEN '2TM'
                 ELSE ANY_VALUE(t.team_abbrev) END                               AS team,
            COUNT(DISTINCT gp.game_pk)                                           AS g,
            SUM(gp.games_started)                                                AS gs,
            SUM(gp.wins)                                                          AS w,
            SUM(gp.losses)                                                        AS l,
            SUM(gp.saves)                                                         AS sv,
            SUM(gp.holds)                                                         AS hld,
            SUM(gp.blown_saves)                                                   AS bs,
            SUM(gp.outs)                                                          AS outs,
            SUM(gp.hits_allowed)                                                  AS h,
            SUM(gp.runs_allowed)                                                  AS r,
            SUM(gp.earned_runs)                                                   AS er,
            SUM(gp.home_runs_allowed)                                             AS hr,
            SUM(gp.walks)                                                          AS bb,
            SUM(gp.strikeouts)                                                    AS so,
            -- ERA = ER * 27 / outs  (since IP = outs/3, ERA = ER/IP*9)
            ROUND(SUM(gp.earned_runs) * 27.0
                  / NULLIF(SUM(gp.outs), 0), 2)                                  AS era,
            -- WHIP = (BB + H) / IP = (BB + H) * 3 / outs
            ROUND((SUM(gp.walks) + SUM(gp.hits_allowed)) * 3.0
                  / NULLIF(SUM(gp.outs), 0), 3)                                  AS whip,
            -- K/9
            ROUND(SUM(gp.strikeouts) * 27.0
                  / NULLIF(SUM(gp.outs), 0), 1)                                  AS k9,
            -- BB/9
            ROUND(SUM(gp.walks) * 27.0
                  / NULLIF(SUM(gp.outs), 0), 1)                                  AS bb9
        FROM silver.game_pitching  gp
        JOIN silver.games          sg ON gp.game_pk   = sg.game_pk
        JOIN silver.players         p ON gp.player_id  = p.player_id
        JOIN gold.dim_team          t ON gp.team_id    = t.team_id
                                     AND sg.season_year = t.season_year
        WHERE sg.season_year = {season}
          AND sg.game_type IN {game_type_sql}
          AND sg.status = 'Final'
          {shared_extra}
        GROUP BY gp.player_id, p.full_name
        {role_filter}
    """

    try:
        df_pit = conn.execute(pitching_sql).df()
    except Exception as exc:
        conn.close()
        st.error(f"Pitching query error: {exc}")
        st.stop()

# Close connection — all queries complete
conn.close()

# ═══════════════════════════════════════════════════════════════════════════════
# RENDER HITTING
# ═══════════════════════════════════════════════════════════════════════════════

# Column display names used for both the DataFrame and Styler highlight
BAT_COL_NAMES: dict[str, str] = {
    "rank": "#", "player": "Player", "team": "Team",
    "g": "G", "ab": "AB", "r": "R", "h": "H",
    "doubles": "2B", "triples": "3B", "hr": "HR", "rbi": "RBI",
    "bb": "BB", "so": "SO",
    "avg": "AVG", "obp": "OBP", "slg": "SLG", "ops": "OPS",
}
# Map sort column key → display header name (for Styler highlight)
BAT_SORT_TO_HEADER: dict[str, str] = {
    "hr": "HR", "avg": "AVG", "ops": "OPS", "rbi": "RBI",
    "obp": "OBP", "slg": "SLG", "h": "H", "r": "R",
    "ab": "AB", "doubles": "2B", "triples": "3B", "bb": "BB",
    "so": "SO", "g": "G",
}

with hit_tab:
    if position != "All Positions":
        df_bat = df_bat[df_bat["pos"] == position]
    if bat_min_ab > 0:
        df_bat = df_bat[df_bat["ab"] >= bat_min_ab]

    if df_bat.empty:
        st.info("No batting data matches the current filters.")
    else:
        df_bat = df_bat.sort_values(bat_col, ascending=not bat_desc, na_position="last")
        df_bat.insert(0, "rank", df_bat[bat_col].rank(
            method="dense", ascending=not bat_desc, na_option="bottom"
        ).astype(int))

        if bat_col in {"avg", "obp", "slg", "ops"} and bat_min_ab == 0:
            st.caption("Tip: set Min AB to hide players with too few plate appearances.")

        # ── Combine player name + position into one column (e.g. "Yordan Alvarez  DH")
        df_bat["player"] = (
            df_bat["full_name"] + "  " + df_bat["pos"].fillna("")
        ).str.strip()

        # ── Format rate stats as ".341" (no leading zero) matching MLB.com style
        def _fmt_rate(val: float | None) -> str:
            if val is None or pd.isna(val):
                return "—"
            # OPS can exceed 1.000 — keep leading digit if so
            if abs(val) >= 1:
                return f"{val:.3f}"
            return f".{int(round(val * 1000)):03d}"

        for stat in ("avg", "obp", "slg", "ops"):
            df_bat[stat] = df_bat[stat].apply(_fmt_rate)

        # ── Build display DataFrame with renamed headers
        disp = df_bat[list(BAT_COL_NAMES)].rename(columns=BAT_COL_NAMES)

        # ── Highlight the sorted column with a light background via Styler
        sort_header = BAT_SORT_TO_HEADER.get(bat_col, "")
        styled = disp.style.hide(axis="index")
        if sort_header in disp.columns:
            styled = styled.set_properties(
                subset=[sort_header],
                **{"background-color": "#e8ecf5"},
            )

        st.dataframe(styled, use_container_width=True)
        st.caption(f"{len(df_bat):,} players — sorted by {bat_sort_label}")

# ═══════════════════════════════════════════════════════════════════════════════
# RENDER PITCHING
# ═══════════════════════════════════════════════════════════════════════════════

with pitch_tab:
    if pit_min_ip > 0:
        df_pit = df_pit[df_pit["outs"] >= pit_min_ip * 3]

    # Compute IP display string in Python (avoids DuckDB HUGEINT division quirks)
    df_pit["ip"] = df_pit["outs"].apply(lambda o: f"{int(o) // 3}.{int(o) % 3}")

    if df_pit.empty:
        st.info("No pitching data matches the current filters.")
    else:
        df_pit = df_pit.sort_values(pit_col, ascending=not pit_desc, na_position="last")
        df_pit.insert(0, "rank", df_pit[pit_col].rank(
            method="dense", ascending=not pit_desc, na_option="bottom"
        ).astype(int))

        if pit_col in {"era", "whip"} and pit_min_ip == 0:
            st.caption("Tip: set Min IP to hide pitchers with too few innings.")

        st.dataframe(
            df_pit[["rank", "full_name", "team",
                    "g", "gs", "w", "l", "sv", "hld", "bs",
                    "ip", "h", "r", "er", "hr", "bb", "so",
                    "era", "whip", "k9", "bb9"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "rank":      st.column_config.NumberColumn("#",      width=40),
                "full_name": st.column_config.TextColumn("Player",  width=170),
                "team":      st.column_config.TextColumn("Team",    width=56),
                "g":         st.column_config.NumberColumn("G",     width=44),
                "gs":        st.column_config.NumberColumn("GS",    width=44),
                "w":         st.column_config.NumberColumn("W",     width=40),
                "l":         st.column_config.NumberColumn("L",     width=40),
                "sv":        st.column_config.NumberColumn("SV",    width=44),
                "hld":       st.column_config.NumberColumn("HLD",   width=48),
                "bs":        st.column_config.NumberColumn("BS",    width=40),
                "ip":        st.column_config.TextColumn("IP",      width=54),
                "h":         st.column_config.NumberColumn("H",     width=44),
                "r":         st.column_config.NumberColumn("R",     width=44),
                "er":        st.column_config.NumberColumn("ER",    width=44),
                "hr":        st.column_config.NumberColumn("HR",    width=44),
                "bb":        st.column_config.NumberColumn("BB",    width=44),
                "so":        st.column_config.NumberColumn("SO",    width=44),
                "era":       st.column_config.NumberColumn("ERA",   format="%.2f", width=60),
                "whip":      st.column_config.NumberColumn("WHIP",  format="%.3f", width=64),
                "k9":        st.column_config.NumberColumn("K/9",   format="%.1f", width=56),
                "bb9":       st.column_config.NumberColumn("BB/9",  format="%.1f", width=60),
            },
        )
        st.caption(f"{len(df_pit):,} pitchers — sorted by {pit_sort_label}")
