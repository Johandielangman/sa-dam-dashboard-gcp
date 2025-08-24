# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~
#      /\_/\
#     ( o.o )
#      > ^ <
#
# Author: Johan Hanekom
# Date: February 2025

# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~

# =========== // STANDARD IMPORTS // ===========

import time
import datetime
import os
from urllib.parse import quote
from typing import (
    Optional,
    Union,
    Tuple,
    List,
    Dict
)

# =========== // CUSTOM IMPORTS // ===========

import pymongo.synchronous.collection
from streamlit_folium import st_folium
import pymongo.synchronous
import streamlit as st
import pandas as pd
import pymongo
import folium


# =========== // CONSTANTS // ===========

start_time = time.time()  # to check if the script is taking long
LOCAL: bool = False
PALETTE = [
    "#e60000",
    "#ffaa02",
    "#fffe03",
    "#4de600",
    "#0959df"
]

TABLE_COLUMNS = {
    "dam": "Dam Name",
    "province": "Province",
    "river": "River",
    "full_storage_capacity": "FSC Million m³",
    "this_week": "Current %",
    "last_year": "Last Year %",
    "wall_height_m": "Wall Height (m)",
    "year_completed": "Year Built",
    "nearest_locale": "Nearest Town"
}

# =========== // MAIN PAGE SETUP // ===========

st.set_page_config(
    page_title="Dam Dash",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Removes the "Deploy" and menu button
# This was suggested on a forum, although I heard from a friend
# that this could have been done in a config file
if not LOCAL:
    hide_streamlit_style: str = """
        <style>
            #MainMenu {visibility: hidden;}
            .stAppDeployButton {display:none;}
            footer {visibility: hidden;}
        </style>
    """
    st.markdown(
        hide_streamlit_style,
        unsafe_allow_html=True
    )

# =========== // HELPER FUNCTIONS // ===========


def get_color(
    value: Union[int, float]
) -> str:
    if value < 25:
        return PALETTE[0]
    elif value < 50:
        return PALETTE[1]
    elif value < 75:
        return PALETTE[2]
    elif value < 90:
        return PALETTE[3]
    else:
        return PALETTE[4]

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

# =========== // GET FILTER OPTIONS // ===========


def get_latest_report_date() -> Optional[datetime.datetime]:
    latest_date: datetime.datetime = client['dam-dash']['reports'].find_one(
        sort=[("report_date", -1)],
        projection={"report_date": 1}
    )
    return latest_date["report_date"] if (
        latest_date
    ) else None


@st.cache_data(ttl="600s")
def get_filter_options() -> Tuple[List[datetime.datetime], List[str]]:
    reports: pymongo.synchronous.collection.Collection = client['dam-dash']['reports']
    report_dates: List[datetime.datetime] = sorted(
        reports.distinct("report_date"),
        reverse=True
    )
    provinces: List[str] = sorted(
        reports.distinct("province")
    )
    return report_dates, provinces

# =========== // DATA FETCH // ===========


@st.cache_data(ttl="20s")
def get_data(
    report_date: datetime.datetime,
    province: str
) -> pd.DataFrame:
    query: Dict[str, Union[datetime.datetime, str]] = {}
    if report_date != "All":
        query["report_date"] = report_date

    if province != "All":
        query["province"] = province

    projection_fields = {k: 1 for k in TABLE_COLUMNS.keys()}
    projection_fields.update({"lat_long": 1, "last_week": 1, "_id": 0})

    items: List[Dict] = list(client['dam-dash']['reports'].find(
        filter=query,
        projection=projection_fields
    ))

    df: pd.DataFrame = pd.DataFrame(items)
    df.rename(columns=TABLE_COLUMNS, inplace=True)

    # Convert to millions for FSC
    df[TABLE_COLUMNS['full_storage_capacity']] = df[TABLE_COLUMNS['full_storage_capacity']] / 1e6

    # Compute percentage change
    df["Weekly Change"] = df.apply(
        lambda row: (
            f'🔼 {row[TABLE_COLUMNS["this_week"]] - row["last_week"]:.1f}%' if (
                row[TABLE_COLUMNS["this_week"]] > row["last_week"]
            ) else (
                f'🔻 {row[TABLE_COLUMNS["this_week"]] - row["last_week"]:.1f}%'
            ) if (
                row[TABLE_COLUMNS["this_week"]] < row["last_week"]
            ) else '◼ 0%'
        ), axis=1)

    # Add numeric change for metrics
    df["Change_Numeric"] = df[TABLE_COLUMNS["this_week"]] - df["last_week"]

    # Clean up
    df.drop(columns=["last_week"], inplace=True, errors='ignore')

    return df

# =========== // MAIN PAGE LAYOUT // ===========


# Header
st.title("South Africa Dam Dashboard 💧")

# Filters moved to main page
col1, col2, col3 = st.columns([2, 2, 1])

report_dates, provinces = get_filter_options()

with col1:
    report_date: datetime.datetime = st.selectbox(
        label="📅 Report Date",
        options=["All"] + report_dates,
        index=1 if get_latest_report_date() in report_dates else 0
    )

with col2:
    province: str = st.selectbox(
        label="🌍 Province",
        options=["All"] + provinces
    )

with col3:
    display_date: str = pd.to_datetime(
        report_date
    ).strftime("%d %b %Y") if (
        report_date != "All"
    ) else "All Dates"
    st.metric("Selected Date", display_date)

with st.spinner('Fetching data...'):
    data: pd.DataFrame = get_data(
        report_date=report_date,
        province=province
    )

if report_date != "All":
    st.write(f"📊 **{len(data)} dams** found for your selection")
else:
    st.write(f"📊 **{len(data)} dam reports** found for your selection across all dates")

# =========== // METRICS // ===========

st.markdown("---")
if not data.empty and "Change_Numeric" in data.columns and report_date != "All":
    col1, col2 = st.columns(2)
    # Find highest and lowest increase
    highest_increase = data.loc[data["Change_Numeric"].idxmax()]
    lowest_increase = data.loc[data["Change_Numeric"].idxmin()]

    with col1:
        st.metric(
            label="🔝 Biggest Increase",
            value=f"{highest_increase[TABLE_COLUMNS['dam']]}",
            delta=f"{highest_increase['Change_Numeric']:.1f}%"
        )

    with col2:
        st.metric(
            label="🔻 Biggest Decrease",
            value=f"{lowest_increase[TABLE_COLUMNS['dam']]}",
            delta=f"{lowest_increase['Change_Numeric']:.1f}%"
        )
    st.markdown("---")


# =========== // TABS LAYOUT // ===========

(
    tab1,
    tab2,
    tab3,
    tab4
) = st.tabs(["📊 Data Table", "🗺️ Interactive Map", "📈 Analysis", "ℹ️ About"])

with tab1:
    st.subheader("📊 Dam Levels Data")

    data_display = data.copy()
    data_display.sort_values(
        by=[
            TABLE_COLUMNS['province'],
            TABLE_COLUMNS['this_week']
        ],
        ascending=[True, False],
        inplace=True
    )

    main_cols = [
        TABLE_COLUMNS['dam'],
        TABLE_COLUMNS['province'],
        TABLE_COLUMNS['river'],
        TABLE_COLUMNS['this_week'],
        "Weekly Change",
        TABLE_COLUMNS['full_storage_capacity']
    ]

    st.dataframe(
        data_display[main_cols],
        hide_index=True,
        use_container_width=True
    )

    st.subheader("🤓 Interesting Facts")
    detail_cols = [
        TABLE_COLUMNS['dam'],
        TABLE_COLUMNS['last_year'],
        TABLE_COLUMNS['wall_height_m'],
        TABLE_COLUMNS['year_completed'],
        TABLE_COLUMNS['nearest_locale']
    ]
    available_cols = [col for col in detail_cols if col in data_display.columns]
    if available_cols:
        st.dataframe(
            data_display[available_cols],
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("Additional details not available for this dataset.")

    csv = data_display.to_csv(index=False)
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name=f"dam_data_{display_date.replace(' ', '_')}.csv",
        mime='text/csv'
    )

with tab2:
    st.subheader("Dam Locations Map")

    if report_date == "All":
        st.warning('Map view is disabled when "All" dates are selected. Please choose a specific date.', icon="ℹ️")
    else:
        # =========== // NORMALIZE THE DOT SIZE // ===========
        min_size, max_size = 6, 15
        min_fsc = data[TABLE_COLUMNS['full_storage_capacity']].min()
        max_fsc = data[TABLE_COLUMNS['full_storage_capacity']].max()

        def get_marker_size(fsc):
            return min_size + (max_size - min_size) * ((fsc - min_fsc) / (max_fsc - min_fsc) if max_fsc > min_fsc else 0)

        # =========== // CREATE MAP // ===========
        m = folium.Map(
            location=[-28, 24],
            zoom_start=6,
            tiles='OpenStreetMap'
        )
        m.fit_bounds([[-35, 16.5], [-22, 33]])

        # Add markers
        missing_dams: int = 0
        for _, row in data.iterrows():
            if (
                not all([loc for loc in row["lat_long"]]) or
                not isinstance(row["lat_long"], list) or
                len(row["lat_long"]) != 2
            ):
                missing_dams += 1
                continue

            folium.CircleMarker(
                location=row["lat_long"],
                radius=get_marker_size(row[TABLE_COLUMNS['full_storage_capacity']]),
                color=get_color(row[TABLE_COLUMNS['this_week']]),
                fill=True,
                fill_color=get_color(row[TABLE_COLUMNS['this_week']]),
                fill_opacity=0.8,
                popup=f"<b>{row[TABLE_COLUMNS['dam']]}</b><br>"
                      f"Current: {row[TABLE_COLUMNS['this_week']]}%<br>"
                      f"River: {row[TABLE_COLUMNS['river']]}"
            ).add_to(m)

        st_folium(
            m,
            height=500,
            use_container_width=True,
            zoom=5,
            returned_objects=[]
        )

        if missing_dams:
            st.warning(f"⚠️ {missing_dams} dams missing location data")

        st.markdown(f"""
        **Map Legend:**
        - <span style='color:{PALETTE[0]};'>● Very Low (0-25%)</span>
        - <span style='color:{PALETTE[1]};'>● Moderately Low (25-50%)</span>
        - <span style='color:{PALETTE[2]};'>● Near Normal (50-75%)</span>
        - <span style='color:{PALETTE[3]};'>● Moderately High (75-90%)</span>
        - <span style='color:{PALETTE[4]};'>● High (90%+)</span>

        *Dot size represents dam storage capacity*
        """, unsafe_allow_html=True)

with tab3:
    st.subheader("📈 Historical Trends")

    st.markdown("""
    **Analyze dam water levels over time** 🌊

    The Historical Trends page provides:
    - 📅 **Custom date range selection**
    - 🏞️ **Multi-dam comparison**
    - 📊 **Interactive charts with zoom and pan**
    - 📈 **Statistical analysis and insights**
    - 📥 **Data export capabilities**

    Click the button below to explore historical trends in detail.
    """)
    st.markdown("---")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("📈 View Historical Trends", type="primary", use_container_width=True):
            st.switch_page("pages/historical_trends.py")

with tab4:
    st.subheader("About This Dashboard")

with tab4:
    st.subheader("About This Dashboard")

    st.markdown("""
    **Welcome to the South African Dam Dashboard!**

    This dashboard provides weekly updates on dam levels across South Africa, with data sourced from the
    [Department of Water and Sanitation](https://www.dws.gov.za/hydrology/Weekly/Province.aspx).

    ### Features:
    - 📊 **Data Table**: Sortable table with current levels, weekly changes, and storage capacity
    - 🗺️ **Interactive Map**: Visual representation with color-coded dam levels
    - 📥 **Download**: Export data as CSV for further analysis
    - 🔍 **Detailed View**: Additional dam information including construction details

    ### How to Use:
    1. Select a report date and province using the filters above
    2. Browse the data table or explore the interactive map
    3. Click on map markers for detailed dam information
    4. Download data for offline analysis

    ### Data Notes:
    - **Weekly Change**: Compares current week to previous week (🔼 increase, 🔻 decrease)
    - **FSC**: Full Storage Capacity in million cubic meters
    - **Map Colors**: Represent current fill percentage (see legend on map tab)
    - **Dot Size**: Proportional to dam storage capacity
    """)

    # Support link
    st.markdown("---")
    st.markdown("**Support this project:**")
    st.markdown("[![BuyMeACoffee](https://img.shields.io/badge/Buy_Me_A_Coffee-FFDD00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/johanlangman)")

    # Performance info
    if time.time() - start_time > 10.0:
        st.info("💡 **Performance Note**: This dashboard runs on a personal server. If loading seems slow, that's why! Thanks for your patience.")

# =========== // CLEANUP // ===========

if time.time() - start_time > 10.0:
    msg = st.toast('Hi!', icon="🛑")
    time.sleep(3)
    msg.toast('If things feel slow...', icon="🛑")
    time.sleep(3)
    msg.toast('Hang tight!', icon="🛑")
    time.sleep(3)
    msg.toast('Thanks! And enjoy!', icon="🎉")
