# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~
#      /\_/\
#     ( o.o )
#      > ^ <
#
# Author: Johan Hanekom
# Date: February 2025
# Page: Historical Trends

# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~

import os
import time
import datetime
from urllib.parse import quote
from typing import List, Dict, Optional

import streamlit as st
import pandas as pd
import pymongo
import plotly.express as px

# =========== // PAGE SETUP // ===========

st.set_page_config(
    page_title="Historical Trends - Dam Dash",
    page_icon="📈",
    layout="wide"
)

# =========== // MONGO CONNECTION // ===========


@st.cache_resource(ttl='30s')
def init_connection() -> pymongo.MongoClient:
    return pymongo.MongoClient(
        f"mongodb+srv://"
        f"{quote(os.environ['MONGO_USERNAME'], safe='')}:"
        f"{quote(os.environ['MONGO_PASSWORD'], safe='')}@"
        f"{os.environ['MONGO_CLUSTER']}"
    )


client: pymongo.MongoClient = init_connection()

# =========== // DATA FUNCTIONS // ===========


@st.cache_data(ttl="300s")
def get_all_dams() -> List[str]:
    """Get list of all available dams"""
    dams = client['dam-dash']['reports'].distinct("dam")
    return sorted(dams)


@st.cache_data(ttl="300s")
def get_date_range() -> tuple:
    """Get the available date range"""
    reports = client['dam-dash']['reports']
    min_date = reports.find_one(sort=[("report_date", 1)])["report_date"]
    max_date = reports.find_one(sort=[("report_date", -1)])["report_date"]
    return min_date, max_date


@st.cache_data(ttl="300s")
def get_historical_data(
    dam_names: List[str],
    start_date: datetime.datetime,
    end_date: datetime.datetime
) -> pd.DataFrame:
    query = {
        "dam": {"$in": dam_names},
        "report_date": {"$gte": start_date, "$lte": end_date}
    }
    projection = {
        "dam": 1,
        "report_date": 1,
        "this_week": 1,
        "province": 1,
        "river": 1,
        "_id": 0
    }

    items = list(client['dam-dash']['reports'].find(
        filter=query,
        projection=projection
    ))

    df = pd.DataFrame(items)
    if not df.empty:
        df['report_date'] = pd.to_datetime(df['report_date'])
        df = df.sort_values(['dam', 'report_date'])

    return df

# =========== // MAIN PAGE // ===========

# Header
st.title("📈 Historical Dam Trends")
st.markdown("Analyze water level trends over time for multiple dams")

# Navigation
col1, col2 = st.columns([1, 4])
with col1:
    if st.button("← Back to Dashboard", type="secondary"):
        st.switch_page("main.py")

# Get available data
min_date, max_date = get_date_range()
all_dams = get_all_dams()

# Controls
st.subheader("🎛️ Selection Controls")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**📅 Date Range**")
    start_date = st.date_input(
        "Start Date",
        value=max_date - datetime.timedelta(days=180),  # Default to last 6 months
        min_value=min_date.date(),
        max_value=max_date.date(),
        key="start_date"
    )
    
    end_date = st.date_input(
        "End Date", 
        value=max_date.date(),
        min_value=min_date.date(),
        max_value=max_date.date(),
        key="end_date"
    )

with col2:
    st.markdown("**🏞️ Dam Selection**")
    selected_dams = st.multiselect(
        "Choose dams to compare:",
        options=all_dams,
        default=[],
        help="Select one or more dams to analyze their trends",
        key="dam_selection"
    )

# Validation
if start_date > end_date:
    st.error("❌ Start date must be before end date")
    st.stop()

if not selected_dams:
    st.info("👆 Please select at least one dam to view historical trends")
    st.stop()

with st.spinner('Loading historical data...'):
    start_datetime = datetime.datetime.combine(start_date, datetime.time.min)
    end_datetime = datetime.datetime.combine(end_date, datetime.time.max)
    historical_data = get_historical_data(selected_dams, start_datetime, end_datetime)

if historical_data.empty:
    st.warning("No data found for the selected dams and date range")
    st.stop()

# =========== // VISUALIZATIONS // ===========

st.subheader("📊 Water Level Trends")

fig = px.line(
    historical_data,
    x='report_date',
    y='this_week',
    color='dam',
    title='Historical Water Levels Over Time',
    labels={
        'report_date': 'Date',
        'this_week': 'Water Level (%)',
        'dam': 'Dam Name'
    },
    hover_data=['province', 'river']
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Water Level (%)",
    hovermode='x unified',
    height=600,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    )
)

fig.update_layout(
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ])
        ),
        type="date"
    )
)

fig.add_hline(y=25, line_dash="dash", line_color="red", annotation_text="Low (25%)")
fig.add_hline(y=50, line_dash="dash", line_color="orange", annotation_text="Moderate (50%)")
fig.add_hline(y=75, line_dash="dash", line_color="green", annotation_text="Good (75%)")

st.plotly_chart(fig, use_container_width=True)

# =========== // STATISTICS // ===========

st.subheader("📊 Summary Statistics")

stats_data = []
for dam in selected_dams:
    dam_data = historical_data[historical_data['dam'] == dam]['this_week']
    if not dam_data.empty:
        stats_data.append({
            'Dam': dam,
            'Province': historical_data[historical_data['dam'] == dam]['province'].iloc[0],
            'River': historical_data[historical_data['dam'] == dam]['river'].iloc[0],
            'Min %': dam_data.min(),
            'Max %': dam_data.max(),
            'Average %': dam_data.mean(),
            'Current %': dam_data.iloc[-1] if len(dam_data) > 0 else None,
            'Std Dev': dam_data.std(),
            'Data Points': len(dam_data)
        })

stats_df = pd.DataFrame(stats_data)

for col in ['Min %', 'Max %', 'Average %', 'Current %', 'Std Dev']:
    if col in stats_df.columns:
        stats_df[col] = stats_df[col].round(1)

st.dataframe(stats_df, use_container_width=True, hide_index=True)
