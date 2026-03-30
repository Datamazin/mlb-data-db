import duckdb
con = duckdb.connect("data/gold/mlb.duckdb")
con.sql("SHOW TABLES").fetchall()
con.sql("SELECT * FROM some_table LIMIT 5").fetchall()
