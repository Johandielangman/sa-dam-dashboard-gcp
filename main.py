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
    "this_week": "Pct Filled",
}

# =========== // MAIN PAGE SETUP // ===========

st.set_page_config(
    page_title="Dam Dash",
    page_icon="favicon.svg",
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
    else:
        TABLE_COLUMNS.update({"report_date": "Date"})

    if province != "All":
        query["province"] = province

    items: List[Dict] = list(client['dam-dash']['reports'].find(
        filter=query,
        projection={
            k: 1 for k in TABLE_COLUMNS.keys()
        } | {"lat_long": 1, "last_week": 1}
    ))

    df: pd.DataFrame = pd.DataFrame(items)
    df.rename(
        columns=TABLE_COLUMNS,
        inplace=True
    )
    df[TABLE_COLUMNS['full_storage_capacity']] = df[TABLE_COLUMNS['full_storage_capacity']] / 1e6

    # Compute percentage change
    df["Change"] = df.apply(
        lambda row: (
            f'🔼 {row["Pct Filled"] - row["last_week"]:.1f}%' if (
                row["Pct Filled"] > row["last_week"]
            ) else (
                f'🔻 {row["Pct Filled"] - row["last_week"]:.1f}%'
            ) if (
                row["Pct Filled"] < row["last_week"]
            )else '◼ 0%'
        ), axis=1)

    # No longer needed
    df.drop(
        columns=["last_week"],
        inplace=True
    )

    return df

# =========== // FILTERS (in the sidebar) // ===========


report_dates, provinces = get_filter_options()
report_date: datetime.datetime = st.sidebar.selectbox(
    label="Select Report Date",
    options=["All"] + report_dates,
    index=1 if get_latest_report_date() in report_dates else 0
)

province: str = st.sidebar.selectbox(
    label="Select Province",
    options=["All"] + provinces
)

display_date: str = pd.to_datetime(
    report_date
).strftime("%d %B %Y") if (
    report_date != "All"
) else "All Dates"

# The landing text
st.title("South Africa Dam Dashboard 💧")
st.write(f"### Report Date: **{display_date}** 📆")
st.write("**Welcome to the South African Dam Dashboard!** This dashboard provides weekly updates on dam levels across South Africa, with data sourced from the [Department of Water and Sanitation](https://www.dws.gov.za/hydrology/Weekly/Province.aspx). The information is presented in both a table and an interactive map. By default, the table is sorted by province and percentage filled. Filters are available in the sidebar, allowing you to refine the data by report date and province. By default, the latest available data is shown for all provinces. On the map, each dot represents a dam location. The color of the dot indicates the dam's water level (see the legend in the sidebar), while the dot's size reflects the dam's storage capacity—larger dots represent larger dams. The table also includes a **difference column**, comparing each dam's current percentage filled to the previous week's value, indicating whether the level has increased 🔼 or decreased 🔻. Feel free to download the data as a CSV.")

with st.spinner('Fetching data...'):
    data: pd.DataFrame = get_data(
        report_date=report_date,
        province=province
    )

# =========== // BUILD THE MAIN PAGE // ===========

left_column, right_column = st.columns(
    [1, 2],
    gap="medium"
)

# >>>>>>>>>>>>>>>>>>>>> LEFT COLUMN
with left_column:
    st.write("#### Dam Levels Table 📊")
    data.sort_values(
        by=[
            TABLE_COLUMNS['province'],
            TABLE_COLUMNS['this_week']
        ],
        ascending=[True, False],
        inplace=True
    )

    st.dataframe(
        data[list(TABLE_COLUMNS.values()) + ["Change"]],
        hide_index=True
    )
    st.write(f"Reporting {len(data)} dams!")

    # Shameless plug
    st.write("[![BuyMeACoffee](https://img.shields.io/badge/Buy_Me_A_Coffee-FFDD00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/johanlangman)")


# >>>>>>>>>>>>>>>>>>>>> RIGHT COLUMN
with right_column:
    st.write("#### Dam Levels Map 🌍")

    if report_date == "All":
        st.warning('Since you selected all historic Dam Data, the map is disabled. A date column is added to the table.', icon="ℹ️")
    else:
        # =========== // NORMALIZE THE DOT SIZE // ===========

        min_size, max_size = 6, 15
        min_fsc, max_fsc = data[TABLE_COLUMNS['full_storage_capacity']].min(), data[TABLE_COLUMNS['full_storage_capacity']].max()

        def get_marker_size(fsc):
            return min_size + (max_size - min_size) * ((fsc - min_fsc) / (max_fsc - min_fsc) if max_fsc > min_fsc else 0)

        # =========== // CREATE A FOLIUM MAP CLASS // ===========

        m = folium.Map(
            location=[-28, 24],
            zoom_start=6,
            tiles='OpenStreetMap'
        )
        m.fit_bounds([
            [-35, 16.5],
            [-22, 33]
        ])

        # =========== // ADD CIRCLES TO THE MAP // ===========
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
                popup=f"{row[TABLE_COLUMNS['dam']]} ({row[TABLE_COLUMNS['this_week']]}%)"
            ).add_to(m)

        # =========== // INITIALIZE THE MAP ON STREAMLIT // ===========

        st_folium(
            m,
            height=500,
            use_container_width=True,
            returned_objects=[]  # IMPORTANT! Make it a static plot. Don't want callbacks
        )

        if missing_dams:
            st.warning(f"{missing_dams} dams are missing location data and could not be plotted on the map. This could mean that new dams have been added to DWS. We'll get the location data as soon as possible! Hang tight!", icon="⚠️")

# =========== // LEGEND (sidebar) // ===========

# You can inject HTML?!
st.sidebar.markdown(f"""
### Legend
- <span style='color:{PALETTE[0]};'>● Very Low (0-25)</span>
- <span style='color:{PALETTE[1]};'>● Moderately Low (25-50)</span>
- <span style='color:{PALETTE[2]};'>● Near Normal (50-75)</span>
- <span style='color:{PALETTE[3]};'>● Moderately High (75-90)</span>
- <span style='color:{PALETTE[4]};'>● High (90+)</span>
""", unsafe_allow_html=True)


# =========== // SORRY IF IT TOOK SO LONG // ===========
# I have slow laptop!

if time.time() - start_time > 10.0:
    msg = st.toast('Hi!', icon="🛑")
    time.sleep(3)
    msg.toast('If things feel slow...', icon="🛑")
    time.sleep(3)
    msg.toast('Remember that this is hosted on an old laptop!', icon="🛑")
    time.sleep(3)
    msg.toast('Thanks! And enjoy!', icon="🎉")