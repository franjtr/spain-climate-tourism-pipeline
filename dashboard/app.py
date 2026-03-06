import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path

# Add project root to path so we can import utils
sys.path.append(str(Path(__file__).parent.parent))
from ingestion.utils import get_db_engine

# Page config
st.set_page_config(
    page_title="Spain Climate & Tourism",
    page_icon="🌞",
    layout="wide"
)

@st.cache_data
def load_data():
    engine = get_db_engine()
    df = pd.read_sql("SELECT * FROM mart_weather_tourism", engine)
    return df

df = load_data()

# Sidebar filters
st.sidebar.title("Filters")
years = sorted(df["year"].unique())
selected_year = st.sidebar.selectbox("Year", years, index=len(years)-1)
selected_metric = st.sidebar.selectbox("Tourism metric", ["tourists", "overnight_stays"])

# Filter data
df_year = df[df["year"] == selected_year]

# Title
st.title("🌞 Spain Climate & Tourism Dashboard")
st.markdown(f"Exploring how weather affects tourism across Spanish provinces — {selected_year}")

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Total Tourists", f"{df_year['tourists'].fillna(0).sum():,.0f}")
col2.metric("Total Overnight Stays", f"{df_year['overnight_stays'].fillna(0).sum():,.0f}")
col3.metric("Avg Temperature (Spain)", f"{df_year['avg_temp'].mean():.1f}°C")

st.divider()

# Row 1: Bar chart + Scatter
col1, col2 = st.columns(2)

with col1:
    st.subheader("Tourism by Province")
    df_map = df_year.groupby("province_name")[selected_metric].sum().reset_index()
    df_map = df_map.dropna(subset=[selected_metric])
    df_map = df_map.sort_values(selected_metric, ascending=True)
    fig_bar = px.bar(
        df_map,
        x=selected_metric,
        y="province_name",
        orientation="h",
        color=selected_metric,
        color_continuous_scale="Oranges",
        title=f"{selected_metric.replace('_', ' ').title()} by Province"
    )
    fig_bar.update_layout(height=600)
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.subheader("Temperature vs Tourism")
    df_scatter = df_year.dropna(subset=["avg_temp", selected_metric])
    fig_scatter = px.scatter(
        df_scatter,
        x="avg_temp",
        y=selected_metric,
        color="ccaa",
        hover_name="province_name",
        trendline="ols",
        labels={
            "avg_temp": "Avg Temperature (°C)",
            selected_metric: selected_metric.replace("_", " ").title()
        },
        title="Does warmer weather mean more tourism?"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

st.divider()

# Row 2: Line chart by province with dual Y axis
st.subheader("Monthly Evolution by Province")
provinces = sorted(df["province_name"].dropna().unique())
selected_province = st.selectbox("Select province", provinces)

df_province = df[df["province_name"] == selected_province].sort_values(["year", "month"])
df_province["date"] = pd.to_datetime(df_province[["year", "month"]].assign(day=1))

fig_line = make_subplots(specs=[[{"secondary_y": True}]])

fig_line.add_trace(
    go.Scatter(
        x=df_province["date"],
        y=df_province[selected_metric],
        name=selected_metric.replace("_", " ").title(),
        line=dict(color="steelblue")
    ),
    secondary_y=False
)

fig_line.add_trace(
    go.Scatter(
        x=df_province["date"],
        y=df_province["avg_temp"],
        name="Avg Temp (°C)",
        line=dict(dash="dash", color="red")
    ),
    secondary_y=True
)

fig_line.update_yaxes(title_text=selected_metric.replace("_", " ").title(), secondary_y=False)
fig_line.update_yaxes(title_text="Temperature (°C)", secondary_y=True)
fig_line.update_layout(
    title=f"{selected_province} — Tourism vs Temperature over time",
    hovermode="x unified"
)

st.plotly_chart(fig_line, use_container_width=True)