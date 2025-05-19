import streamlit as st

import pandas as pd
from datetime import datetime, timedelta
import pyarrow.dataset as ds
import s3fs

from dotenv import load_dotenv
import os

import plotly.express as px
import geopandas as gpd


load_dotenv()
ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY")
SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY")
lakefs_endpoint = os.getenv("LAKEFS_ENDPOINT", "http://lakefs-dev:8000")

fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={'endpoint_url': lakefs_endpoint}
)

BASE_DIR = os.getcwd()

### ________________________

# โหลด Data
@st.cache_data(ttl=300)
def load_data(lakefs_path):

    dataset = ds.dataset(
        lakefs_path,
        format="parquet",
        partitioning="hive",
        filesystem=fs
    )
    table = dataset.to_table()
    df = table.to_pandas()

    # รวม column ปีเดือนวันเป็น timestamp (ถ้ายังไม่มี timestamp อยู่)
    # if "timestamp" not in df.columns and all(col in df.columns for col in ["year", "month", "day", "hour"]):
    #     df["timestamp"] = pd.to_datetime(df[["year", "month", "day", "hour"]])

    # กรองแค่ 3 วันล่าสุด
    max_time = df["timestamp"].max()
    start_time = max_time - pd.Timedelta(days=1)
    df_recent = df[df["timestamp"] >= start_time]

    return df_recent
    
    # return df

@st.cache_data()
def load_gdf(geojson_path):
    gdf = gpd.read_file(geojson_path)
    return gdf


####_______________________________________

# อ่านไฟล์ pollution
pollution_path = 'pollution-data/main/pollution.parquet'
pollution_df = load_data(pollution_path)
pollution_df = pollution_df.rename(columns={"components_pm2_5": "pm25"})

coord_path = os.path.join(BASE_DIR, "save", "district_coord.csv")
df_code = pd.read_csv(coord_path)
# df_code = df_code.rename(columns={"district_en":"district", "province_en":"province"})

# st.write(pollution_df)

# merge province_id
pollution_df = pd.merge(
    pollution_df,
    df_code[["province_th", "district_th", "province_id", "district_id"]],
    on="district_id",
    how="left"
)

# โหลดไฟล์ geojson

province_geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_1_clean.geojson")
province_gdf = load_gdf(province_geojson_path)

district_geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2_clean.geojson")
district_gdf = load_gdf(district_geojson_path)


#### _____________________________________________
#__________ Title ________________
# st.set_page_config(page_title="Choropleth Map", page_icon="🗺️")
st.title("แผนที่ค่าฝุ่น PM2.5 รายอำเภอ")

#__________ AQI __________________

def get_aqi_level(pm25):
    if 0 < pm25 <= 12:
        return "Good"
    elif 12 < pm25 <= 35.4:
        return "Moderate"
    elif 35.4 < pm25 <= 55.4:
        return "Unhealthy for Sensitive Groups"
    elif 55.4 < pm25 <= 150.4:
        return "Unhealthy"
    elif 150.4 < pm25 <= 250.4:
        return "Very Unhealthy"
    elif 250.4 < pm25 <= 350.4:
        return "Hazardous"
    elif 350.4 < pm25 <= 500.4:
        return "Very Hazardous"

# pollution_df["aqi_level"] = pollution_df["pm25"].apply(get_aqi_level)

color_map = {
    "Good": "green",
    "Moderate": "yellow",
    "Unhealthy for Sensitive Groups": "orange",
    "Unhealthy": "red",
    "Very Unhealthy": "purple",
    "Hazardous": "maroon",
    "Very Hazardous": "brown"
}

aqi_continuous_colors = [
    [0.0, "#00e400"],     # Good: Green
    [0.1, "#ffff00"],     # Moderate: Yellow
    [0.2, "#ff7e00"],     # USG: Orange
    [0.5, "#ff0000"],     # Unhealthy: Red
    [1.0, "#8f3f97"]      # Very Unhealthy: Purple
]




#__________ Time Filter __________

# ปรับ timestamp ให้อยู่ในช่วง 15 นาที
pollution_df["localtime"] = pd.to_datetime(pollution_df["localtime"])
pollution_df["local_timestamp_15min"] = pollution_df["localtime"].dt.floor("15min")

# ชุดเวลาที่มีจริง (unique, sorted)
all_times = pollution_df["local_timestamp_15min"].sort_values().unique()

if "selected_time" not in st.session_state:
    st.session_state["selected_time"] = all_times[-1]

selected_time = st.select_slider(
    "เลือกเวลา",
    options=all_times,
    value=st.session_state["selected_time"],
    format_func=lambda x: x.strftime("%Y-%m-%d %H:%M")
)

# ช่วงเวลา +-1 hr
window = pd.Timedelta(hours=1)
start = selected_time - window
end   = selected_time + window

mask = pollution_df["local_timestamp_15min"].between(start, end)
df_window = pollution_df[mask]


#________ Radio ________

# ตั้งค่า default ครั้งแรก
if "selected_level" not in st.session_state:
    st.session_state["selected_level"] = "จังหวัด (Province)"

# แสดง radio พร้อมผูกกับ session_state
level = st.radio(
    "ระดับแผนที่",
    ["จังหวัด (Province)", "อำเภอ (District)"],
    index=["จังหวัด (Province)", "อำเภอ (District)"].index(st.session_state["selected_level"]),
    key="selected_level"  # ผูกกับ session_state
)

#________ MAP ______________

if level == "จังหวัด (Province)":
    map_df = (df_window
    .groupby(["local_timestamp_15min", "province_id", "province_th"], as_index=False)
    .agg(pm25=("pm25", "mean"))
)
    map_df["aqi_level"] = map_df["pm25"].apply(get_aqi_level)

    geojson = province_gdf
    locations = "province_id"
    featureidkey = "properties.CC_1"  # ตรวจสอบว่า GeoJSON มี CC_1

    hover_name = "province_th"
    hover_data={
        "province_th": False,
        "pm25": ':.2f' ,
        "province_id": False,
        "local_timestamp_15min": False
    }
    hovertemplate = (
        "<b>จังหวัด : </b> %{customdata[0]}<br>"
        "<b>PM2.5 : </b> %{customdata[1]:.2f}<extra></extra>"
    )
    customdata = map_df[["province_th", "pm25"]].values

elif level == "อำเภอ (District)":
#     map_df = (df_window
#     .groupby(["local_timestamp_15min", "district_id", "province_th", "district_th"], as_index=False)
#     .agg(pm25=("pm25", "mean"))
# )

    map_df = (
    df_window
    .sort_values("timestamp")
    .drop_duplicates(["local_timestamp_15min", "district_id"])
)
    map_df["aqi_level"] = map_df["pm25"].apply(get_aqi_level)

    geojson = district_gdf
    locations = "district_id"
    featureidkey = "properties.CC_2"  # ตรวจสอบว่า GeoJSON มี CC_2

    hover_name = "district_th"
    hover_data={
        "province_th": True,
        "pm25": ':.2f',
        "district_id": False,
        "local_timestamp_15min": False
    }
    hovertemplate = (
        "<b>จังหวัด :</b> %{customdata[0]}<br>"
        "<b>อำเภอ :</b> %{customdata[1]}<br>"
        "<b>PM2.5 :</b> %{customdata[2]:.2f}<extra></extra>"
    )
    customdata = map_df[["province_th", "district_th", "pm25"]].values
    

# plot
fig = px.choropleth_mapbox(
    map_df,
    geojson=geojson,
    locations=locations,
    featureidkey=featureidkey,
    color="pm25",
    color_continuous_scale="YlOrRd",
    # color_continuous_scale=aqi_continuous_colors,
    range_color=(0, 100),
    mapbox_style="carto-positron",
    zoom=5,
    center={"lat": 13.5, "lon": 100.5},
    opacity=0.6,
    labels={"pm25": "ค่าฝุ่น PM2.5 ", "province_th": "จังหวัด "},
    # hover_name=hover_name,
    # hover_data=hover_data,
    animation_frame="local_timestamp_15min"

)

# แก้ pop up
fig.update_traces(
    hovertemplate=hovertemplate,
    customdata=customdata,
)

st.plotly_chart(fig, use_container_width=True)

#######
# Score Card
# Time Serie
# Interactive
# Alert
# date
# backgroud
# UI