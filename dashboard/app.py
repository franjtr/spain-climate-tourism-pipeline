import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from statsmodels.tsa.seasonal import STL
import sys
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Spain Climate & Tourism Intelligence",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .block-container {padding-top: 1rem; padding-bottom: 2rem;}
    div[data-testid="metric-container"] {
        background-color: #f0f2f6;
        border: 1px solid #d6d6d6;
        padding: 10px;
        border-radius: 8px;
        color: #262730;
    }
    </style>
""", unsafe_allow_html=True)

# Data loading
sys.path.append(str(Path(__file__).parent.parent))
from ingestion.utils import get_db_engine  

@st.cache_data
def load_data():
    engine = get_db_engine()
    df = pd.read_sql("SELECT * FROM mart_weather_tourism", engine)
    
    if 'date' not in df.columns:
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    
    return df

df = load_data()

# Auxiliar functions
@st.cache_data
def get_avg_temp_by_year(df, year):
    """Calcula temperatura promedio nacional para un año."""
    df_year = df[df["year"] == year]
    if df_year.empty:
        return 0
    return df_year.groupby('province_name')['avg_temp'].mean().mean()

@st.cache_data
def get_yearly_totals(df, year, metric_col):
    """Calcula totales y temperatura para un año."""
    df_year = df[df["year"] == year]
    total = df_year[metric_col].sum() if not df_year.empty else 0
    avg_temp = get_avg_temp_by_year(df, year)
    return total, avg_temp

# Sidebar
with st.sidebar:
    st.header("🎛️ Control Panel")
    
    years = sorted(df["year"].unique())
    selected_year = st.selectbox("Select Year", years, index=len(years)-1)
    
    metric_options = {
        "tourists": "Tourist Arrivals",
        "overnight_stays": "Overnight Stays"
    }
    selected_metric_col = st.selectbox(
        "Metric", 
        list(metric_options.keys()), 
        format_func=lambda x: metric_options[x]
    )
    metric_label = metric_options[selected_metric_col]

    st.divider()
    
    st.subheader("📊 Analysis Settings")
    exclude_covid = st.checkbox(
        "Remove 2020-2021 from Correlation",
        value=True,
        help="COVID-19 distorted the temperature-tourism relationship."
    )
    
    st.divider()

    with st.expander("🛠️ Pipeline Architecture", expanded=True):
        st.markdown("""
        **Data Engineering Stack:**
        - **Ingestion:** Python (AEMET & INE APIs)
        - **Transformation:** dbt (Data Build Tool)
        - **Containerization:** Docker
        - **Orchestration:** Python Scripts
        - **Visualization:** Streamlit + Plotly
        """)
        st.info("Full historical data: 2019-2025")

# Data processing
df_current = df[df["year"] == selected_year]

total_current, avg_temp_current = get_yearly_totals(df, selected_year, selected_metric_col)
total_prev, avg_temp_prev = get_yearly_totals(df, selected_year - 1, selected_metric_col)

delta_val = ((total_current - total_prev) / total_prev * 100) if total_prev > 0 else 0
delta_temp = avg_temp_current - avg_temp_prev

st.title(f"Spain Tourism & Climate Dashboard ({selected_year})")
st.markdown("An end-to-end data engineering project analyzing the correlation between meteorology and tourism demand.")

# KPIs
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric(label=metric_label, value=f"{total_current:,.0f}", delta=f"{delta_val:.1f}% YoY")
kpi2.metric(label="Avg Temperature", value=f"{avg_temp_current:.1f}°C", delta=f"{delta_temp:.1f}°C YoY")
kpi3.metric(label="Provinces Analyzed", value=df_current['province_name'].nunique())
kpi4.metric(label="Data Points", value=f"{len(df_current):,}")

st.divider()

# TABS
tab1, tab2, tab3 = st.tabs([
    "🗺️ Geospatial Overview", 
    "📈 Time Series Decomposition", 
    "📊 Statistical Correlation"
])

# TAB 1: TREEMAP 
with tab1:
    st.subheader(f"National Distribution of {metric_label}")
    
    df_tree = df_current.groupby(['ccaa', 'province_name']).agg({
        selected_metric_col: 'sum',
        'avg_temp': 'mean'
    }).reset_index()
    
    df_tree['avg_temp'] = df_tree['avg_temp'].round(1)
    df_tree = df_tree.dropna(subset=[selected_metric_col])

    fig_tree = px.treemap(
        df_tree,
        path=[px.Constant("Spain"), 'ccaa', 'province_name'],
        values=selected_metric_col,
        color='avg_temp',
        color_continuous_scale='RdYlBu_r',
        title=f"Tourism Distribution & Avg Temperature ({selected_year})",
        hover_data={'avg_temp': False, selected_metric_col: False}
    )
    
    fig_tree.update_traces(
        root_color="lightgrey",
        hovertemplate='<b>%{label}</b><br>Tourism: %{value:,.0f}<extra></extra>'
    )
    fig_tree.update_coloraxes(colorbar_title="Avg Temp (°C)")
    fig_tree.update_layout(margin=dict(t=30, l=0, r=0, b=0))
    
    st.plotly_chart(fig_tree, width="stretch")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🌡️ National Avg Temp", f"{avg_temp_current:.1f}°C")
    
    with col2:
        hottest = df_tree.loc[df_tree['avg_temp'].idxmax()]
        st.metric("🔥 Hottest Province", hottest['province_name'], delta=f"{hottest['avg_temp']}°C", delta_color="inverse")
    
    with col3:
        coldest = df_tree.loc[df_tree['avg_temp'].idxmin()]
        st.metric("❄️ Coldest Province", coldest['province_name'], delta=f"{coldest['avg_temp']}°C")

# TAB 2: TIME SERIES 
with tab2:
    selected_prov_ts = st.selectbox(
        "Select Province for Deep Analysis", 
        sorted(df["province_name"].unique()), 
        index=sorted(df["province_name"].unique()).index("Madrid") if "Madrid" in df["province_name"].unique() else 0
    )

    st.subheader(f"Time Series Decomposition: {selected_prov_ts}")
    
    st.markdown("""
    **STL Decomposition** breaks down the tourism signal into three components:
    - **Trend:** Long-term growth or decline
    - **Seasonality:** Repeating annual patterns (summer peaks, winter lows)
    - **Residuals:** Unexpected events (COVID-19, strikes, special events)
    
    *Formula:* `Observed = Trend + Seasonality + Residuals`
    """)
    
    df_ts = df[df["province_name"] == selected_prov_ts].sort_values("date").set_index("date")
    ts_data = df_ts[selected_metric_col].asfreq('MS')
    
    missing_count = ts_data.isna().sum()
    
    if missing_count > 0:
        st.warning(f"⚠️ {missing_count} missing months detected. Using interpolation.")
        ts_data = ts_data.interpolate(method='linear').ffill().bfill()

    if len(ts_data) >= 24:
        try:
            stl = STL(ts_data, period=12, robust=True)
            decomposition = stl.fit()
            
            trim = 3
            
            fig_ts = make_subplots(
                rows=4, cols=1, 
                shared_xaxes=True,
                vertical_spacing=0.05,
                subplot_titles=("Observed", "Trend", "Seasonality", "Residuals")
            )
            
            fig_ts.add_trace(go.Scatter(x=ts_data.index, y=ts_data.values, name='Observed', line=dict(color='#1f77b4', width=1)), row=1, col=1)
            fig_ts.add_trace(go.Scatter(x=decomposition.trend.iloc[trim:-trim].index, y=decomposition.trend.iloc[trim:-trim].values, name='Trend', line=dict(color='#ff7f0e', width=3)), row=2, col=1)
            fig_ts.add_trace(go.Scatter(x=decomposition.seasonal.index, y=decomposition.seasonal.values, name='Seasonal', line=dict(color='#2ca02c', width=1.5)), row=3, col=1)
            fig_ts.add_trace(go.Scatter(x=decomposition.resid.iloc[trim:-trim].index, y=decomposition.resid.iloc[trim:-trim].values, name='Residuals', mode='markers+lines', line=dict(color='#d62728', width=0.5)), row=4, col=1)
            
            fig_ts.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5, row=4, col=1)
            fig_ts.update_layout(height=800, showlegend=False, hovermode="x unified")
            
            st.plotly_chart(fig_ts, width="stretch")
            
            
            # INSIGHTS 
            st.divider()
            st.subheader("🔍 Key Insights")
            
            # Metrics
            observed_by_month = ts_data.groupby(ts_data.index.month).mean()
            seasonal_by_month = decomposition.seasonal.groupby(decomposition.seasonal.index.month).mean()
            resid_trimmed = decomposition.resid.iloc[trim:-trim]
            trend_trimmed = decomposition.trend.iloc[trim:-trim]
            
            # Month
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            
            # Row 1
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                busiest_month_idx = observed_by_month.idxmax()
                busiest_month_name = month_names[busiest_month_idx - 1]
                busiest_value = observed_by_month.max()
                st.metric(
                    "📈 Busiest Month", 
                    busiest_month_name,
                    delta=f"{busiest_value:,.0f} avg"
                )
            
            with col2:
                quietest_month_idx = observed_by_month.idxmin()
                quietest_month_name = month_names[quietest_month_idx - 1]
                quietest_value = observed_by_month.min()
                st.metric(
                    "📉 Quietest Month", 
                    quietest_month_name,
                    delta=f"{quietest_value:,.0f} avg",
                    delta_color="inverse"
                )
            
            with col3:
                worst_date = resid_trimmed.idxmin()
                worst_value = resid_trimmed.min()
                st.metric(
                    "⚠️ Biggest Drop", 
                    worst_date.strftime('%b %Y'),
                    delta=f"{worst_value:,.0f}",
                    delta_color="inverse"
                )
            
            with col4:
                best_date = resid_trimmed.idxmax()
                best_value = resid_trimmed.max()
                st.metric(
                    "🚀 Biggest Spike", 
                    best_date.strftime('%b %Y'),
                    delta=f"+{best_value:,.0f}"
                )
            
            # Row 2
            st.divider()
            col_trend, col_seasonal = st.columns(2)
            
            with col_trend:
                st.markdown("##### 📊 Trend Analysis")
    
            # Compare annual average of the trend
            trend_by_year = trend_trimmed.groupby(trend_trimmed.index.year).mean()
    
            first_year = trend_by_year.index[0]
            last_year = trend_by_year.index[-1]
    
            trend_change_pct = ((trend_by_year.iloc[-1] - trend_by_year.iloc[0]) / trend_by_year.iloc[0]) * 100
    
            if trend_change_pct > 0:
                st.success(f"📈 **Overall growth:** +{trend_change_pct:.1f}% ({first_year} vs {last_year})")
            else:
                st.error(f"📉 **Overall decline:** {trend_change_pct:.1f}% ({first_year} vs {last_year})")
    
            # COVID impact
            if 2019 in trend_by_year.index and 2020 in trend_by_year.index:
                covid_impact = ((trend_by_year[2020] - trend_by_year[2019]) / trend_by_year[2019]) * 100
                st.warning(f"🦠 **COVID impact (2020):** {covid_impact:.1f}%")
            
            with col_seasonal:
                st.markdown("##### 🔄 Seasonal Pattern")
                
                # Seasonality bar chart
                fig_seasonal = go.Figure()
                
                colors = ['#e74c3c' if v > 0 else '#3498db' for v in seasonal_by_month.values]
                
                fig_seasonal.add_trace(go.Bar(
                    x=month_names,
                    y=seasonal_by_month.values,
                    marker_color=colors,
                    text=[f"{v:+,.0f}" for v in seasonal_by_month.values],
                    textposition='outside'
                ))
                
                fig_seasonal.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                fig_seasonal.update_layout(
                    height=250,
                    margin=dict(t=10, b=10, l=10, r=10),
                    xaxis_title="",
                    yaxis_title="Seasonal Effect",
                    showlegend=False
                )
                
                st.plotly_chart(fig_seasonal, width="stretch")
                
                # Seasonality insight
                peak_month_idx = seasonal_by_month.idxmax()
                peak_month_name = month_names[peak_month_idx - 1]
                low_month_idx = seasonal_by_month.idxmin()
                low_month_name = month_names[low_month_idx - 1]
                
                seasonality_range = seasonal_by_month.max() - seasonal_by_month.min()
                st.caption(f"🔺 Peak: **{peak_month_name}** (+{seasonal_by_month.max():,.0f}) | 🔻 Low: **{low_month_name}** ({seasonal_by_month.min():,.0f})")
                st.caption(f"📏 Seasonal amplitude: **{seasonality_range:,.0f}** {metric_label.lower()}")
            
            # Expandable explanation
            with st.expander("📖 How to interpret these charts", expanded=False):
                st.markdown("""
                | Component | What it shows | Example |
                |-----------|---------------|---------|
                | **Observed** | Raw data exactly as recorded | Actual tourist counts per month |
                | **Trend** | Underlying growth/decline over years | Tourism growing 5% annually until COVID |
                | **Seasonality** | Predictable annual pattern that repeats | August always +50%, January always -30% |
                | **Residuals** | Anomalies unexplained by trend/season | COVID crash in April 2020, special events |
                
                ---
                
                **Key insight:** Large residuals indicate unexpected events that weren't just normal seasonality.
                """)
            
        except Exception as e:
            st.error(f"Error in decomposition: {e}")
    else:
        st.warning("Not enough data (requires 24+ months).")

# TAB 3: CORRELATION  
with tab3:
    st.subheader("Does Temperature Drive Tourism?")
    
    df_corr_source = df[~df["year"].isin([2020, 2021])] if exclude_covid else df
    years_label = "2019, 2022-2025" if exclude_covid else "2019-2025"
    
    correlations = []
    for prov in df_corr_source["province_name"].unique():
        df_p = df_corr_source[df_corr_source["province_name"] == prov].dropna(subset=["avg_temp", selected_metric_col])
        
        if len(df_p) > 12:
            correlations.append({
                "province": prov, 
                "ccaa": df_p["ccaa"].iloc[0],
                "correlation": df_p["avg_temp"].corr(df_p[selected_metric_col]),
                "total_volume": df_p[selected_metric_col].sum()
            })
    
    df_corr = pd.DataFrame(correlations).sort_values("correlation")

    col_chart, col_data = st.columns([2, 1])

    with col_chart:
        fig_corr = px.bar(
            df_corr, x="correlation", y="province", orientation="h",
            color="correlation", color_continuous_scale="RdBu_r", range_color=[-0.8, 0.8],
            title=f"Correlation: Temp vs Tourism ({years_label})"
        )
        fig_corr.add_vline(x=0, line_dash="dash", line_color="black", opacity=0.3)
        fig_corr.update_layout(height=1000)
        st.plotly_chart(fig_corr, width="stretch")

    with col_data:
        st.metric("📈 Avg Correlation", f"{df_corr['correlation'].mean():.3f}")
        st.metric("🗺️ Provinces", len(df_corr))
        
        st.divider()
        
        st.write("##### 🔥 Top 5 Weather-Dependent")
        st.dataframe(df_corr.nlargest(5, "correlation")[["province", "correlation"]], hide_index=True)
        
        st.write("##### ❄️ Bottom 5 Weather-Dependent")
        st.dataframe(df_corr.nsmallest(5, "correlation")[["province", "correlation"]], hide_index=True)