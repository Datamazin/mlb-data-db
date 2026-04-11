"""Players — searchable player directory from gold.dim_player."""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from app import get_conn

st.set_page_config(page_title="Players — MLB Analytics", layout="wide")
st.title("Players")

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

PAGE_SIZE = 50

# ── Position code → description mapping ──────────────────────────────────────
POSITION_LABELS: dict[str, str] = {
    "1":  "Pitcher",
    "2":  "Catcher",
    "3":  "First Base",
    "4":  "Second Base",
    "5":  "Third Base",
    "6":  "Shortstop",
    "7":  "Left Field",
    "8":  "Center Field",
    "9":  "Right Field",
    "10": "Designated Hitter",
    "I":  "Infielder",
    "O":  "Outfielder",
    "Y":  "Two-Way Player",
}

# ── Filters ───────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

with col1:
    search = st.text_input("Search by name", placeholder="e.g. Shohei")

raw_positions = [r[0] for r in conn.execute(
    "SELECT DISTINCT primary_position FROM gold.dim_player "
    "WHERE primary_position IS NOT NULL ORDER BY primary_position"
).fetchall()]
position_options = ["All"] + [POSITION_LABELS.get(p, p) for p in raw_positions]
position_code_map = {POSITION_LABELS.get(p, p): p for p in raw_positions}

countries = ["All"] + [r[0] for r in conn.execute(
    "SELECT DISTINCT birth_country FROM gold.dim_player "
    "WHERE birth_country IS NOT NULL ORDER BY birth_country"
).fetchall()]

with col2:
    position_label = st.selectbox("Position", position_options)

with col3:
    country = st.selectbox("Country", countries)

with col4:
    active_only = st.checkbox("Active only", value=True)

# ── Build WHERE clause ────────────────────────────────────────────────────────
where = []
params: list = []

if search:
    where.append("LOWER(full_name) LIKE ?")
    params.append(f"%{search.lower()}%")

if position_label != "All":
    where.append("primary_position = ?")
    params.append(position_code_map[position_label])

if country != "All":
    where.append("birth_country = ?")
    params.append(country)

if active_only:
    where.append("active = true")

where_clause = ("WHERE " + " AND ".join(where)) if where else ""

# ── Total count (for pagination) ──────────────────────────────────────────────
total = conn.execute(
    f"SELECT COUNT(*) FROM gold.dim_player {where_clause}", params
).fetchone()[0]

total_pages = max(1, math.ceil(total / PAGE_SIZE))

# Reset to page 1 whenever filters change
filter_key = (search, position_label, country, active_only)
if st.session_state.get("_players_filter_key") != filter_key:
    st.session_state["_players_filter_key"] = filter_key
    st.session_state["players_page"] = 1

page = st.session_state.get("players_page", 1)

# ── Fetch one page ────────────────────────────────────────────────────────────
offset = (page - 1) * PAGE_SIZE
df = conn.execute(f"""
    SELECT
        full_name,
        primary_position AS position,
        bats,
        throws,
        birth_date,
        birth_city,
        birth_country,
        height,
        weight,
        mlb_debut_date,
        active
    FROM gold.dim_player
    {where_clause}
    ORDER BY last_name, first_name
    LIMIT {PAGE_SIZE} OFFSET {offset}
""", params).df()
conn.close()
df["position"] = df["position"].map(lambda p: POSITION_LABELS.get(p, p) if p else p)

# ── Results header ────────────────────────────────────────────────────────────
st.write(f"{total:,} players — page {page} of {total_pages}")

st.dataframe(
    df,
    width="stretch",
    hide_index=True,
    column_config={
        "full_name":      "Name",
        "position":       "Pos",
        "bats":           "Bats",
        "throws":         "Throws",
        "birth_date":     st.column_config.DateColumn("Born"),
        "birth_city":     "City",
        "birth_country":  "Country",
        "height":         "Height",
        "weight":         "Weight",
        "mlb_debut_date": st.column_config.DateColumn("MLB Debut"),
        "active":         "Active",
    },
)

# ── Pagination controls ───────────────────────────────────────────────────────
if total_pages > 1:
    pcol1, pcol2, pcol3 = st.columns([1, 2, 1])
    with pcol1:
        if st.button("← Previous", disabled=page <= 1):
            st.session_state["players_page"] = page - 1
            st.rerun()
    with pcol2:
        st.markdown(
            f"<div style='text-align:center; padding-top:8px'>"
            f"Page {page} of {total_pages}</div>",
            unsafe_allow_html=True,
        )
    with pcol3:
        if st.button("Next →", disabled=page >= total_pages):
            st.session_state["players_page"] = page + 1
            st.rerun()
