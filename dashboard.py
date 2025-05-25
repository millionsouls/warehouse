import streamlit as st
import pandas as pd
import sqlite3

st.title("NYPD Arrest Data Warehouse")

# Connect to DB
conn = sqlite3.connect("warehouse.db")

# Load top offenses
df = pd.read_sql("""
    SELECT arrest_boro, COUNT(*) as count
    FROM arrests
    GROUP BY arrest_boro
    ORDER BY count DESC
""", conn)

conn.close()

st.bar_chart(df.set_index("arrest_boro"))
