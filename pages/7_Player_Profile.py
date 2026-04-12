"""Player profile page with season/career filters and splits."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import get_conn

st.set_page_config(page_title="Player Profile - MLB Analytics", layout="wide")

st.markdown("""
<style>
/* Active tab — blue text + bold */
div[data-baseweb="tab-list"] button[aria-selected="true"] p {
    color: #1565C0 !important;
    font-weight: 700 !important;
}
/* Active tab indicator bar — blue */
div[data-baseweb="tab-highlight"] {
    background-color: #1565C0 !important;
}
/* Inactive tab hover — blue tint background + blue text */
div[data-baseweb="tab-list"] button[aria-selected="false"]:hover {
    background-color: #e3f2fd !important;
}
div[data-baseweb="tab-list"] button[aria-selected="false"]:hover p {
    color: #1565C0 !important;
}
</style>
""", unsafe_allow_html=True)

GAME_TYPE_MAP: dict[str, str] = {
    "Regular Season": "('R')",
    "Postseason": "('F','D','L','W')",
    "All": "('R','F','D','L','W')",
}


def _fmt_rate(val: float | int | str | None) -> str:
    if val is None:
        return "-"
    if pd.isna(val):
        return "-"
    try:
        value = float(val)
    except (TypeError, ValueError):
        return "-"
    if abs(value) >= 1:
        return f"{value:.3f}"
    return f".{int(round(value * 1000)):03d}"


def _fmt_ip(outs: float | int | str | None) -> str:
    if outs is None:
        return "-"
    try:
        value = int(outs)
    except (TypeError, ValueError):
        return "-"
    return f"{value // 3}.{value % 3}"


def _intify(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    return df


def _display_batting(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    result = _intify(result, ["g", "ab", "r", "h", "doubles", "triples", "hr", "rbi", "bb", "so"])
    for stat in ["avg", "obp", "slg", "ops"]:
        if stat in result.columns:
            result[stat] = result[stat].apply(_fmt_rate)
    return result


def _display_pitching(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    result = _intify(
        result,
        ["g", "gs", "w", "l", "sv", "hld", "bs", "h", "r", "er", "hr", "bb", "so"],
    )
    if "outs" in result.columns:
        result["ip"] = result["outs"].apply(_fmt_ip)
    return result


def _season_filter_sql(season: int | str) -> tuple[str, list[int]]:
    if season == "Career":
        return "", []
    return "AND sg.season_year = ?", [int(season)]


def _month_label_sql(season: int | str) -> str:
    if season == "Career":
        return "STRFTIME(DATE_TRUNC('month', sg.game_date), '%Y-%m')"
    return "STRFTIME(DATE_TRUNC('month', sg.game_date), '%b')"


current_player_id = st.session_state.get("profile_player_id")

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

try:
    player_options = conn.execute(
        """
        WITH player_pool AS (
            SELECT DISTINCT player_id FROM silver.game_batting
            UNION
            SELECT DISTINCT player_id FROM silver.game_pitching
        )
        SELECT p.player_id, p.full_name
        FROM silver.players p
        JOIN player_pool pp ON p.player_id = pp.player_id
        ORDER BY p.full_name
        """
    ).fetchall()

    if not player_options:
        conn.close()
        st.info("No players with stats are available in the database.")
        st.stop()

    player_ids = [row[0] for row in player_options]
    if current_player_id not in player_ids:
        current_player_id = player_ids[0]

    top_cols = st.columns([1, 3])
    with top_cols[0]:
        if st.button("<- Back to Leaders"):
            conn.close()
            st.switch_page("pages/6_Leaders.py")
    with top_cols[1]:
        selected_player = st.selectbox(
            "Choose Player",
            player_options,
            index=player_ids.index(current_player_id),
            format_func=lambda option: option[1],
        )

    player_id = int(selected_player[0])
    if player_id != st.session_state.get("profile_player_id"):
        st.session_state["profile_season"] = "Career"
    st.session_state["profile_player_id"] = player_id

    bio_row = conn.execute(
        """
        SELECT
            full_name,
            primary_position,
            bats,
            throws,
            birth_date,
            birth_city,
            birth_country,
            height,
            weight,
            mlb_debut_date
        FROM silver.players
        WHERE player_id = ?
        """,
        [player_id],
    ).fetchone()

    if bio_row is None:
        conn.close()
        st.error(f"Player {player_id} not found.")
        st.stop()

    (
        full_name,
        primary_pos,
        bats,
        throws_hand,
        birth_date,
        birth_city,
        birth_country,
        height,
        weight,
        debut_date,
    ) = bio_row

    seasons = [
        row[0]
        for row in conn.execute(
            """
            WITH player_games AS (
                SELECT game_pk FROM silver.game_batting WHERE player_id = ?
                UNION
                SELECT game_pk FROM silver.game_pitching WHERE player_id = ?
            )
            SELECT DISTINCT sg.season_year
            FROM silver.games sg
            JOIN player_games pg ON sg.game_pk = pg.game_pk
            WHERE sg.status = 'Final'
            ORDER BY sg.season_year DESC
            """,
            [player_id, player_id],
        ).fetchall()
    ]

    season_options: list[int | str] = ["Career", *seasons]
    saved_season = st.session_state.get("profile_season", "Career")
    season_index = season_options.index(saved_season) if saved_season in season_options else 0

    st.title(full_name)

    birth_location = ", ".join(part for part in [birth_city, birth_country] if part)
    bio_cols = st.columns(6)
    bio_fields = [
        ("Position", primary_pos or "-"),
        ("Bats / Throws", f"{bats or '-'} / {throws_hand or '-'}"),
        ("Born Date", str(birth_date) if birth_date else "-"),
        ("Born Location", birth_location or "-"),
        ("Height / Weight", f"{height or '-'}  {weight or '-'} lb"),
        ("MLB Debut", str(debut_date) if debut_date else "-"),
    ]
    for col, (label, value) in zip(bio_cols, bio_fields):
        col.metric(label, value)

    if not seasons:
        conn.close()
        st.info("No stats available for this player in the database.")
        st.stop()

    filter_cols = st.columns([1.5, 1.5, 2])
    with filter_cols[0]:
        season = st.selectbox("Season", season_options, index=season_index)
        st.session_state["profile_season"] = season
    with filter_cols[1]:
        game_type_label = st.selectbox("Game Type", list(GAME_TYPE_MAP), key="profile_game_type")
    with filter_cols[2]:
        st.caption("Home/away and monthly splits use game-level stats. vs LHP/RHP uses the opposing starting pitcher hand.")

    game_type_sql = GAME_TYPE_MAP[game_type_label]
    season_filter_sql, season_params = _season_filter_sql(season)
    month_label_sql = _month_label_sql(season)
    scope_label = str(season)

    teams_row = conn.execute(
        f"""
        SELECT COALESCE(string_agg(team_abbrev, ', '), '-')
        FROM (
            SELECT DISTINCT team_abbrev
            FROM (
                SELECT t.team_abbrev
                FROM silver.game_batting gb
                JOIN silver.games sg ON gb.game_pk = sg.game_pk
                JOIN gold.dim_team t ON gb.team_id = t.team_id AND sg.season_year = t.season_year
                WHERE gb.player_id = ?
                  {season_filter_sql}
                  AND sg.status = 'Final'
                  AND sg.game_type IN {game_type_sql}

                UNION ALL

                SELECT t.team_abbrev
                FROM silver.game_pitching gp
                JOIN silver.games sg ON gp.game_pk = sg.game_pk
                JOIN gold.dim_team t ON gp.team_id = t.team_id AND sg.season_year = t.season_year
                WHERE gp.player_id = ?
                  {season_filter_sql}
                  AND sg.status = 'Final'
                  AND sg.game_type IN {game_type_sql}
            )
            ORDER BY team_abbrev
        )
        """,
        [player_id, *season_params, player_id, *season_params],
    ).fetchone()

    batting_summary = conn.execute(
        f"""
        SELECT
            COUNT(DISTINCT gb.game_pk) AS g,
            SUM(gb.at_bats) AS ab,
            SUM(gb.runs) AS r,
            SUM(gb.hits) AS h,
            SUM(gb.doubles) AS doubles,
            SUM(gb.triples) AS triples,
            SUM(gb.home_runs) AS hr,
            SUM(gb.rbi) AS rbi,
            SUM(gb.walks) AS bb,
            SUM(gb.strikeouts) AS so,
            ROUND(SUM(gb.hits)::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS avg,
            ROUND((SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0), 3) AS obp,
            ROUND((SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS slg,
            ROUND(
                (SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0)
                + (SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0),
                3
            ) AS ops
        FROM silver.game_batting gb
        JOIN silver.games sg ON gb.game_pk = sg.game_pk
        WHERE gb.player_id = ?
          {season_filter_sql}
          AND sg.status = 'Final'
          AND sg.game_type IN {game_type_sql}
        """,
        [player_id, *season_params],
    ).df()

    pitching_summary = conn.execute(
        f"""
        SELECT
            COUNT(DISTINCT gp.game_pk) AS g,
            SUM(gp.games_started) AS gs,
            SUM(gp.wins) AS w,
            SUM(gp.losses) AS l,
            SUM(gp.saves) AS sv,
            SUM(gp.holds) AS hld,
            SUM(gp.blown_saves) AS bs,
            SUM(gp.outs) AS outs,
            SUM(gp.hits_allowed) AS h,
            SUM(gp.runs_allowed) AS r,
            SUM(gp.earned_runs) AS er,
            SUM(gp.home_runs_allowed) AS hr,
            SUM(gp.walks) AS bb,
            SUM(gp.strikeouts) AS so,
            ROUND(SUM(gp.earned_runs) * 27.0 / NULLIF(SUM(gp.outs), 0), 2) AS era,
            ROUND((SUM(gp.walks) + SUM(gp.hits_allowed)) * 3.0 / NULLIF(SUM(gp.outs), 0), 3) AS whip,
            ROUND(SUM(gp.strikeouts) * 27.0 / NULLIF(SUM(gp.outs), 0), 1) AS k9,
            ROUND(SUM(gp.walks) * 27.0 / NULLIF(SUM(gp.outs), 0), 1) AS bb9
        FROM silver.game_pitching gp
        JOIN silver.games sg ON gp.game_pk = sg.game_pk
        WHERE gp.player_id = ?
          {season_filter_sql}
          AND sg.status = 'Final'
          AND sg.game_type IN {game_type_sql}
        """,
        [player_id, *season_params],
    ).df()

    batting_home_away = conn.execute(
        f"""
        SELECT
            CASE WHEN gb.is_home THEN 'Home' ELSE 'Away' END AS split,
            CASE WHEN gb.is_home THEN 1 ELSE 2 END AS sort_order,
            COUNT(DISTINCT gb.game_pk) AS g,
            SUM(gb.at_bats) AS ab,
            SUM(gb.runs) AS r,
            SUM(gb.hits) AS h,
            SUM(gb.doubles) AS doubles,
            SUM(gb.triples) AS triples,
            SUM(gb.home_runs) AS hr,
            SUM(gb.rbi) AS rbi,
            SUM(gb.walks) AS bb,
            SUM(gb.strikeouts) AS so,
            ROUND(SUM(gb.hits)::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS avg,
            ROUND((SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0), 3) AS obp,
            ROUND((SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS slg,
            ROUND(
                (SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0)
                + (SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0),
                3
            ) AS ops
        FROM silver.game_batting gb
        JOIN silver.games sg ON gb.game_pk = sg.game_pk
        WHERE gb.player_id = ?
          {season_filter_sql}
          AND sg.status = 'Final'
          AND sg.game_type IN {game_type_sql}
        GROUP BY 1, 2
        ORDER BY 2
        """,
        [player_id, *season_params],
    ).df()

    pitching_home_away = conn.execute(
        f"""
        SELECT
            CASE WHEN gp.is_home THEN 'Home' ELSE 'Away' END AS split,
            CASE WHEN gp.is_home THEN 1 ELSE 2 END AS sort_order,
            COUNT(DISTINCT gp.game_pk) AS g,
            SUM(gp.games_started) AS gs,
            SUM(gp.wins) AS w,
            SUM(gp.losses) AS l,
            SUM(gp.saves) AS sv,
            SUM(gp.holds) AS hld,
            SUM(gp.blown_saves) AS bs,
            SUM(gp.outs) AS outs,
            SUM(gp.hits_allowed) AS h,
            SUM(gp.runs_allowed) AS r,
            SUM(gp.earned_runs) AS er,
            SUM(gp.home_runs_allowed) AS hr,
            SUM(gp.walks) AS bb,
            SUM(gp.strikeouts) AS so,
            ROUND(SUM(gp.earned_runs) * 27.0 / NULLIF(SUM(gp.outs), 0), 2) AS era,
            ROUND((SUM(gp.walks) + SUM(gp.hits_allowed)) * 3.0 / NULLIF(SUM(gp.outs), 0), 3) AS whip,
            ROUND(SUM(gp.strikeouts) * 27.0 / NULLIF(SUM(gp.outs), 0), 1) AS k9,
            ROUND(SUM(gp.walks) * 27.0 / NULLIF(SUM(gp.outs), 0), 1) AS bb9
        FROM silver.game_pitching gp
        JOIN silver.games sg ON gp.game_pk = sg.game_pk
        WHERE gp.player_id = ?
          {season_filter_sql}
          AND sg.status = 'Final'
          AND sg.game_type IN {game_type_sql}
        GROUP BY 1, 2
        ORDER BY 2
        """,
        [player_id, *season_params],
    ).df()

    batting_vs_hand = conn.execute(
        f"""
        WITH opposing_starter AS (
            SELECT
                gp.game_pk,
                gp.team_id,
                p.throws,
                ROW_NUMBER() OVER (
                    PARTITION BY gp.game_pk, gp.team_id
                    ORDER BY gp.games_started DESC, gp.outs DESC, gp.player_id
                ) AS rn
            FROM silver.game_pitching gp
            JOIN silver.players p ON gp.player_id = p.player_id
        )
        SELECT
            CASE
                WHEN os.throws = 'L' THEN 'vs LHP'
                WHEN os.throws = 'R' THEN 'vs RHP'
                ELSE 'Unknown'
            END AS split,
            CASE
                WHEN os.throws = 'L' THEN 1
                WHEN os.throws = 'R' THEN 2
                ELSE 3
            END AS sort_order,
            COUNT(DISTINCT gb.game_pk) AS g,
            SUM(gb.at_bats) AS ab,
            SUM(gb.runs) AS r,
            SUM(gb.hits) AS h,
            SUM(gb.doubles) AS doubles,
            SUM(gb.triples) AS triples,
            SUM(gb.home_runs) AS hr,
            SUM(gb.rbi) AS rbi,
            SUM(gb.walks) AS bb,
            SUM(gb.strikeouts) AS so,
            ROUND(SUM(gb.hits)::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS avg,
            ROUND((SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0), 3) AS obp,
            ROUND((SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS slg,
            ROUND(
                (SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0)
                + (SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0),
                3
            ) AS ops
        FROM silver.game_batting gb
        JOIN silver.games sg ON gb.game_pk = sg.game_pk
        LEFT JOIN opposing_starter os
               ON gb.game_pk = os.game_pk
              AND gb.team_id <> os.team_id
              AND os.rn = 1
        WHERE gb.player_id = ?
          {season_filter_sql}
          AND sg.status = 'Final'
          AND sg.game_type IN {game_type_sql}
        GROUP BY 1, 2
        ORDER BY 2
        """,
        [player_id, *season_params],
    ).df()

    batting_monthly = conn.execute(
        f"""
        SELECT
            DATE_TRUNC('month', sg.game_date)::DATE AS month_start,
            {month_label_sql} AS month,
            COUNT(DISTINCT gb.game_pk) AS g,
            SUM(gb.at_bats) AS ab,
            SUM(gb.runs) AS r,
            SUM(gb.hits) AS h,
            SUM(gb.doubles) AS doubles,
            SUM(gb.triples) AS triples,
            SUM(gb.home_runs) AS hr,
            SUM(gb.rbi) AS rbi,
            SUM(gb.walks) AS bb,
            SUM(gb.strikeouts) AS so,
            ROUND(SUM(gb.hits)::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS avg,
            ROUND((SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0), 3) AS obp,
            ROUND((SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0), 3) AS slg,
            ROUND(
                (SUM(gb.hits) + SUM(gb.walks))::DOUBLE / NULLIF(SUM(gb.at_bats) + SUM(gb.walks), 0)
                + (SUM(gb.hits) + SUM(gb.doubles) + 2 * SUM(gb.triples) + 3 * SUM(gb.home_runs))::DOUBLE / NULLIF(SUM(gb.at_bats), 0),
                3
            ) AS ops
        FROM silver.game_batting gb
        JOIN silver.games sg ON gb.game_pk = sg.game_pk
        WHERE gb.player_id = ?
          {season_filter_sql}
          AND sg.status = 'Final'
          AND sg.game_type IN {game_type_sql}
        GROUP BY 1, 2
        ORDER BY 1
        """,
        [player_id, *season_params],
    ).df()

    pitching_monthly = conn.execute(
        f"""
        SELECT
            DATE_TRUNC('month', sg.game_date)::DATE AS month_start,
            {month_label_sql} AS month,
            COUNT(DISTINCT gp.game_pk) AS g,
            SUM(gp.games_started) AS gs,
            SUM(gp.wins) AS w,
            SUM(gp.losses) AS l,
            SUM(gp.saves) AS sv,
            SUM(gp.holds) AS hld,
            SUM(gp.blown_saves) AS bs,
            SUM(gp.outs) AS outs,
            SUM(gp.hits_allowed) AS h,
            SUM(gp.runs_allowed) AS r,
            SUM(gp.earned_runs) AS er,
            SUM(gp.home_runs_allowed) AS hr,
            SUM(gp.walks) AS bb,
            SUM(gp.strikeouts) AS so,
            ROUND(SUM(gp.earned_runs) * 27.0 / NULLIF(SUM(gp.outs), 0), 2) AS era,
            ROUND((SUM(gp.walks) + SUM(gp.hits_allowed)) * 3.0 / NULLIF(SUM(gp.outs), 0), 3) AS whip,
            ROUND(SUM(gp.strikeouts) * 27.0 / NULLIF(SUM(gp.outs), 0), 1) AS k9,
            ROUND(SUM(gp.walks) * 27.0 / NULLIF(SUM(gp.outs), 0), 1) AS bb9
        FROM silver.game_pitching gp
        JOIN silver.games sg ON gp.game_pk = sg.game_pk
        WHERE gp.player_id = ?
          {season_filter_sql}
          AND sg.status = 'Final'
          AND sg.game_type IN {game_type_sql}
        GROUP BY 1, 2
        ORDER BY 1
        """,
        [player_id, *season_params],
    ).df()
finally:
    conn.close()

batting_summary = _display_batting(batting_summary)
pitching_summary = _display_pitching(pitching_summary)
batting_home_away = _display_batting(batting_home_away)
pitching_home_away = _display_pitching(pitching_home_away)
batting_vs_hand = _display_batting(batting_vs_hand)
batting_monthly = _display_batting(batting_monthly)
pitching_monthly = _display_pitching(pitching_monthly)

has_batting = not batting_summary.empty and int(batting_summary.iloc[0]["g"] or 0) > 0
has_pitching = not pitching_summary.empty and int(pitching_summary.iloc[0]["g"] or 0) > 0

teams_text = teams_row[0] if teams_row else "-"
role = "Two-way" if has_batting and has_pitching else "Batter" if has_batting else "Pitcher" if has_pitching else "No stats"

st.caption(f"{scope_label} | {game_type_label} | {teams_text} | {role}")

overview_tab, batting_tab, pitching_tab = st.tabs(["Overview", "Batting", "Pitching"])

with overview_tab:
    metric_cols = st.columns(4)
    metric_cols[0].metric("Teams", teams_text)
    metric_cols[1].metric("Role", role)
    metric_cols[2].metric("Batting G", int(batting_summary.iloc[0]["g"]) if has_batting else 0)
    metric_cols[3].metric("Pitching G", int(pitching_summary.iloc[0]["g"]) if has_pitching else 0)
    st.subheader("Batting Summary")
    if has_batting:
        st.dataframe(
            batting_summary[["g", "ab", "r", "h", "doubles", "triples", "hr", "rbi", "bb", "so", "avg", "obp", "slg", "ops"]],
            hide_index=True,
            width="stretch",
            column_config={
                "g": st.column_config.NumberColumn("G", width="small"),
                "ab": st.column_config.NumberColumn("AB", width="small"),
                "r": st.column_config.NumberColumn("R", width="small"),
                "h": st.column_config.NumberColumn("H", width="small"),
                "doubles": st.column_config.NumberColumn("2B", width="small"),
                "triples": st.column_config.NumberColumn("3B", width="small"),
                "hr": st.column_config.NumberColumn("HR", width="small"),
                "rbi": st.column_config.NumberColumn("RBI", width="small"),
                "bb": st.column_config.NumberColumn("BB", width="small"),
                "so": st.column_config.NumberColumn("SO", width="small"),
                "avg": st.column_config.TextColumn("AVG", width="small"),
                "obp": st.column_config.TextColumn("OBP", width="small"),
                "slg": st.column_config.TextColumn("SLG", width="small"),
                "ops": st.column_config.TextColumn("OPS", width="small"),
            },
        )
    else:
        st.info("No batting stats for this filter.")

    st.subheader("Pitching Summary")
    if has_pitching:
        st.dataframe(
            pitching_summary[["g", "gs", "w", "l", "sv", "hld", "bs", "ip", "h", "r", "er", "hr", "bb", "so", "era", "whip", "k9", "bb9"]],
            hide_index=True,
            width="stretch",
            column_config={
                "g": st.column_config.NumberColumn("G", width="small"),
                "gs": st.column_config.NumberColumn("GS", width="small"),
                "w": st.column_config.NumberColumn("W", width="small"),
                "l": st.column_config.NumberColumn("L", width="small"),
                "sv": st.column_config.NumberColumn("SV", width="small"),
                "hld": st.column_config.NumberColumn("HLD", width="small"),
                "bs": st.column_config.NumberColumn("BS", width="small"),
                "ip": st.column_config.TextColumn("IP", width="small"),
                "h": st.column_config.NumberColumn("H", width="small"),
                "r": st.column_config.NumberColumn("R", width="small"),
                "er": st.column_config.NumberColumn("ER", width="small"),
                "hr": st.column_config.NumberColumn("HR", width="small"),
                "bb": st.column_config.NumberColumn("BB", width="small"),
                "so": st.column_config.NumberColumn("SO", width="small"),
                "era": st.column_config.NumberColumn("ERA", format="%.2f", width="small"),
                "whip": st.column_config.NumberColumn("WHIP", format="%.3f", width="small"),
                "k9": st.column_config.NumberColumn("K/9", format="%.1f", width="small"),
                "bb9": st.column_config.NumberColumn("BB/9", format="%.1f", width="small"),
            },
        )
    else:
        st.info("No pitching stats for this filter.")

with batting_tab:
    st.subheader("Batting")
    if has_batting:
        row = batting_summary.iloc[0]
        batting_metrics = st.columns(4)
        batting_metrics[0].metric("AVG", row["avg"])
        batting_metrics[1].metric("OPS", row["ops"])
        batting_metrics[2].metric("HR", int(row["hr"]))
        batting_metrics[3].metric("RBI", int(row["rbi"]))

        st.markdown("**Home / Away**")
        st.dataframe(
            batting_home_away[["split", "g", "ab", "r", "h", "doubles", "triples", "hr", "rbi", "bb", "so", "avg", "obp", "slg", "ops"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "split": "Split",
                "g": "G",
                "ab": "AB",
                "r": "R",
                "h": "H",
                "doubles": "2B",
                "triples": "3B",
                "hr": "HR",
                "rbi": "RBI",
                "bb": "BB",
                "so": "SO",
                "avg": "AVG",
                "obp": "OBP",
                "slg": "SLG",
                "ops": "OPS",
            },
        )

        st.markdown("**vs LHP / RHP**")
        st.dataframe(
            batting_vs_hand[["split", "g", "ab", "r", "h", "doubles", "triples", "hr", "rbi", "bb", "so", "avg", "obp", "slg", "ops"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "split": "Split",
                "g": "G",
                "ab": "AB",
                "r": "R",
                "h": "H",
                "doubles": "2B",
                "triples": "3B",
                "hr": "HR",
                "rbi": "RBI",
                "bb": "BB",
                "so": "SO",
                "avg": "AVG",
                "obp": "OBP",
                "slg": "SLG",
                "ops": "OPS",
            },
        )

        st.markdown("**Monthly Log**")
        st.dataframe(
            batting_monthly[["month", "g", "ab", "r", "h", "doubles", "triples", "hr", "rbi", "bb", "so", "avg", "obp", "slg", "ops"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "month": "Month",
                "g": "G",
                "ab": "AB",
                "r": "R",
                "h": "H",
                "doubles": "2B",
                "triples": "3B",
                "hr": "HR",
                "rbi": "RBI",
                "bb": "BB",
                "so": "SO",
                "avg": "AVG",
                "obp": "OBP",
                "slg": "SLG",
                "ops": "OPS",
            },
        )
    else:
        st.info("No batting stats for this filter.")

with pitching_tab:
    st.subheader("Pitching")
    if has_pitching:
        row = pitching_summary.iloc[0]
        pitching_metrics = st.columns(4)
        pitching_metrics[0].metric("ERA", f"{float(row['era']):.2f}" if not pd.isna(row["era"]) else "-")
        pitching_metrics[1].metric("WHIP", f"{float(row['whip']):.3f}" if not pd.isna(row["whip"]) else "-")
        pitching_metrics[2].metric("SO", int(row["so"]))
        pitching_metrics[3].metric("IP", row["ip"])

        st.markdown("**Home / Away**")
        st.dataframe(
            pitching_home_away[["split", "g", "gs", "w", "l", "sv", "hld", "bs", "ip", "h", "r", "er", "hr", "bb", "so", "era", "whip", "k9", "bb9"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "split": "Split",
                "g": "G",
                "gs": "GS",
                "w": "W",
                "l": "L",
                "sv": "SV",
                "hld": "HLD",
                "bs": "BS",
                "ip": "IP",
                "h": "H",
                "r": "R",
                "er": "ER",
                "hr": "HR",
                "bb": "BB",
                "so": "SO",
                "era": st.column_config.NumberColumn("ERA", format="%.2f"),
                "whip": st.column_config.NumberColumn("WHIP", format="%.3f"),
                "k9": st.column_config.NumberColumn("K/9", format="%.1f"),
                "bb9": st.column_config.NumberColumn("BB/9", format="%.1f"),
            },
        )

        st.markdown("**Monthly Log**")
        st.dataframe(
            pitching_monthly[["month", "g", "gs", "w", "l", "sv", "hld", "bs", "ip", "h", "r", "er", "hr", "bb", "so", "era", "whip", "k9", "bb9"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "month": "Month",
                "g": "G",
                "gs": "GS",
                "w": "W",
                "l": "L",
                "sv": "SV",
                "hld": "HLD",
                "bs": "BS",
                "ip": "IP",
                "h": "H",
                "r": "R",
                "er": "ER",
                "hr": "HR",
                "bb": "BB",
                "so": "SO",
                "era": st.column_config.NumberColumn("ERA", format="%.2f"),
                "whip": st.column_config.NumberColumn("WHIP", format="%.3f"),
                "k9": st.column_config.NumberColumn("K/9", format="%.1f"),
                "bb9": st.column_config.NumberColumn("BB/9", format="%.1f"),
            },
        )
    else:
        st.info("No pitching stats for this filter.")
