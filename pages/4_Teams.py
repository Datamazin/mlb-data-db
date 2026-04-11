"""Teams — team directory grouped by league and division."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from app import get_conn

st.set_page_config(page_title="Teams — MLB Analytics", layout="wide")
st.title("Teams")

conn = get_conn()
if conn is None:
    st.error("Database not available.")
    st.stop()

seasons = [r[0] for r in conn.execute(
    "SELECT DISTINCT season_year FROM gold.dim_team ORDER BY season_year DESC"
).fetchall()]

if not seasons:
    conn.close()
    st.info("No team data yet.")
    st.stop()

season = st.selectbox("Season", seasons)

df = conn.execute("""
    SELECT
        team_name,
        team_abbrev,
        city,
        league_name,
        division_name,
        venue_name,
        first_year,
        active
    FROM gold.dim_team
    WHERE season_year = ?
    ORDER BY league_name, division_name, team_name
""", [season]).df()
conn.close()

for league in ["American League", "National League"]:
    league_df = df[df["league_name"] == league]
    if league_df.empty:
        continue
    st.header(league)

    for division in sorted(league_df["division_name"].unique()):
        div_df = league_df[league_df["division_name"] == division].copy()
        st.subheader(division)
        display = div_df[["team_name", "team_abbrev", "city", "venue_name", "first_year", "active"]]
        display.columns = ["Team", "Abbrev", "City", "Venue", "Est.", "Active"]
        st.dataframe(display, width="stretch", hide_index=True)
