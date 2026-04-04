"""Players — searchable player directory from gold.dim_player."""

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

# ── Filters ───────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    search = st.text_input("Search by name", placeholder="e.g. Shohei")

positions = ["All"] + [r[0] for r in conn.execute(
    "SELECT DISTINCT primary_position FROM gold.dim_player "
    "WHERE primary_position IS NOT NULL ORDER BY primary_position"
).fetchall()]

with col2:
    position = st.selectbox("Position", positions)

with col3:
    active_only = st.checkbox("Active only", value=True)

# ── Query ─────────────────────────────────────────────────────────────────────
where = []
params: list = []

if search:
    where.append("LOWER(full_name) LIKE ?")
    params.append(f"%{search.lower()}%")

if position != "All":
    where.append("primary_position = ?")
    params.append(position)

if active_only:
    where.append("active = true")

where_clause = ("WHERE " + " AND ".join(where)) if where else ""

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
    LIMIT 500
""", params).df()

st.write(f"{len(df):,} players" + (" (capped at 500)" if len(df) == 500 else ""))

st.dataframe(
    df,
    use_container_width=True,
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
