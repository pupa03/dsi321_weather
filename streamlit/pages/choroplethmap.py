import streamlit as st
import folium
from streamlit_folium import st_folium

import pandas as pd
import pyarrow.dataset as ds
import s3fs

import json
from dotenv import load_dotenv
import os

import plotly.express as px
import plotly.graph_objects as go
import geopandas as gpd



st.set_page_config(page_title="Choropleth Map", page_icon="🗺️")

st.title("แผนที่มลพิษรายอำเภอ (Choropleth)")

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

### Function ____________________

# โหลด GeoJSON
@st.cache_data
def load_geojson(geojson_path):
    with open(geojson_path, "r", encoding="utf-8") as f:
        return json.load(f)

# โหลด Data
@st.cache_data
def load_data(lakefs_path):
    dataset = ds.dataset(
        lakefs_path,
        format="parquet",
        partitioning=["year", "month", "day", "hour"],
        filesystem=fs
    )
    table = dataset.to_table()
    df = table.to_pandas()
    
    return df


# weather_path = 's3a://weather-data/main/weather.parquet'
# weather_df = load_data(weather_path)

# @st.cache_data
# def load_weather_data():

#     return df


####________________________________

# geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")

# pollution_path = 'pollution-data/main/pollution.parquet'
# pollution_df = load_data(pollution_path)
# pollution_df = pollution_df.rename(columns={"components_pm2_5": "pm25"})
# st.write('Pollution')
# st.write(pollution_df)
# st.write()



# province_geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_1.json")
# province_geojson = load_geojson(province_geojson_path)

# district_geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")
# district_geojson = load_geojson(district_geojson_path)

####________________________________
# 1. โหลด GeoJSON อำเภอ
# district_geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")
# district_geojson = load_geojson(district_geojson_path)
# with open("../work/save/gadm41_THA_2.json", "r", encoding="utf-8") as f:
#     amphoe_geojson = json.load(f)

# st.write(amphoe_geojson)
# st.write(len(amphoe_geojson))

# 2. โหลดข้อมูลค่ามลพิษ (เช่น pm2.5) รายอำเภอ
# ต้องมีคอลัมน์: "amphoe_code" (หรือรหัสอำเภอที่ตรงกับ GeoJSON) และ "pm25"
# df = pd.read_parquet("save/f65bb697be7843fd9e092d83f914065f-0.parquet", engine="pyarrow")
coord_path = os.path.join(BASE_DIR, "save", "district_coord.csv")
data_path = os.path.join(BASE_DIR, "save", "bd5d4217b47743f2b597ac5cd8293ba0-0.parquet")

df_code = pd.read_csv(coord_path)
df = pd.read_parquet(data_path)

# แสดงตัวอย่างข้อมูล

df_code = df_code.rename(columns={"district_en":"district"})
# st.dataframe(df.head())
# st.dataframe(df_code.head())

df = pd.merge(
    df,
    df_code[["district", "district_id"]],
    on="district",
    how="left"  # ใช้ 'left' เพื่อคงข้อมูล df หลักไว้ทั้งหมด
)

# merge ฝุ่นกับ weather
# merged = pd.merge(
#     weather_df,
#     pollution_df,
#     on=["district_id", "flow_timestamp"],
#     how="inner"
# )

# st.dataframe(df.head())
###______________________
# map ตัวที่ผิดพลาด

# BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
# district_geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")

# district_geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")
# district_geojson = load_geojson(district_geojson_path)
# st.write('district_geojson')
# st.write(district_geojson_path)
# st.write(district_geojson.keys())

geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")
st.write('geojson_path')
st.write(geojson_path) #/app/save/gadm41_THA_2.json
gdf = gpd.read_file(geojson_path)
st.dataframe(gdf.head())

district_id_map = {
    ("BuengKan", "BungKan"): "3801",
    ("BuengKan", "BungKhongLong"): "3806",
    ("BuengKan", "K.BungKhla"): "3808",
    ("BuengKan", "PakKhat"): "3805",
    ("BuengKan", "PhonCharoen"): "3802",
    ("BuengKan", "Seka"): "3804",
    ("BuengKan", "SiWilai"): "3807",
    ("BuengKan", "SoPhisai"): "3803",
    ("Chanthaburi", "MuangChanthaburi"): "2201",
    ("KhonKaen", "WiangKao"): "4029"
}

def map_district_id(row):
    return district_id_map.get((row["NAME_1"], row["NAME_2"]), None)

gdf["district_id"] = gdf.apply(map_district_id, axis=1)

# changed = gdf[gdf["district_id"].notna()]
# print("อำเภอที่ถูกอัปเดต:", changed[["NAME_1", "NAME_2", "CC_2"]])

# เอาค่าใหม่ (district_id จาก mapping) ไปแทนที่ค่าใน CC_2 เฉพาะแถวที่ไม่ใช่ None
gdf["CC_2"] = gdf["district_id"].combine_first(gdf["CC_2"])

# จากนั้นลบ column ชั่วคราว
gdf = gdf.drop(columns=["district_id"])
# gdf[gdf["NAME_1"] == "BuengKan"]



# 3. สร้างแผนที่ Folium
m = folium.Map(location=[13.5, 100.7], zoom_start=6)

# 4. สร้าง Choropleth map
folium.Choropleth(
    geo_data=gdf, # ไฟล์ geojson
    data=df,
    columns=["district_id", "components_pm2_5"],  # รหัสอำเภอ และค่า
    key_on="feature.properties.CC_2",  # เปลี่ยนตามโครงสร้าง GeoJSON
    fill_color="YlOrRd",
    fill_opacity=0.8,
    line_opacity=0,
    legend_name="PM2.5 (µg/m³)",
).add_to(m)

# Optional: ใส่ popup ชื่ออำเภอ
folium.GeoJson(
    gdf,
    name="อำเภอ",
    tooltip=folium.GeoJsonTooltip(
        fields=["NAME_2"],
        aliases=["อำเภอ:"],
        localize=True
    )
).add_to(m)

# 5. แสดงแผนที่ใน Streamlit
st_folium(m, height=700)


####_______
# ดึงรหัสอำเภอทั้งหมดจาก gdf
geo_ids = set(gdf["CC_2"].astype(str))
st.write(f"จำนวนรหัสอำเภอใน GeoDataFrame: {len(geo_ids)}")
st.dataframe(sorted(geo_ids))

# ดึงรหัสอำเภอที่มีใน df (ค่าฝุ่น)
df_ids = set(df["district_id"].dropna().astype(int).astype(str))
st.write(f"จำนวนรหัสอำเภอใน DataFrame ข้อมูลฝุ่น: {len(df_ids)}")
st.dataframe(sorted(df_ids))

# หาอำเภอที่ไม่มีข้อมูล (อยู่ใน geo แต่ไม่อยู่ใน df)
missing_ids = geo_ids - df_ids
st.write(f"จำนวนอำเภอที่ไม่มีค่าฝุ่น: {len(missing_ids)}")
st.write(sorted(missing_ids))

# ดึงชื่ออำเภอ-จังหวัดของอำเภอที่ไม่มีข้อมูล
missing_rows = gdf[gdf["CC_2"].astype(str).isin(missing_ids)]

missing_names = [
    f"{row['CC_2']} - {row['NAME_1']} - {row['NAME_2']}"
    for _, row in missing_rows.iterrows()
]

# แสดงผล
st.markdown(f"### ❌ อำเภอที่ไม่มีค่าฝุ่น (pm2.5): {len(missing_names)} รายการ")
st.write(missing_names)


###
#มี เลขไม่ตรง
# 0:"4303 - BuengKan - BungKan"
# 1:"4311 - BuengKan - BungKhongLong"
# 2:"4313 - BuengKan - K.BungKhla"
# 3:"4310 - BuengKan - PakKhat"
# 4:"4304 - BuengKan - PhonCharoen"
# 5:"4309 - BuengKan - Seka"
# 6:"4312 - BuengKan - SiWilai"
# 7:"4306 - BuengKan - SoPhisai"

# มี แมพเลข
# 9:"#N/A - Chanthaburi - MuangChanthaburi"
# 11:"NA - KhonKaen - WiangKao"

#ไม่มี
# 8:"3123 - BuriRam - Chalermphrakiet"
# 10:"8608 - Chumphon - ThungTako"
# 12:"3032 - NakhonRatchasima - Chalermphrakiet"
# 13:"8023 - NakhonSiThammarat - Chalermphrakiet"

# มี recheck
# 14:"NA - Phatthalung - SongkhlaLake" ควนขนุน พัทลุง
# 15:"NA - Songkhla - SongkhlaLake" ระโนด สงขลา


# สร้าง DataFrame จาก geojson
geo_df = pd.DataFrame([
    {
        "CC_2": feature["properties"]["CC_2"],
        "NAME_1": feature["properties"]["NAME_1"],  # จังหวัด
        "NAME_2": feature["properties"]["NAME_2"]   # อำเภอ
    }
    for feature in gdf["features"]
])

# ปรับชนิดข้อมูลให้แมพกันได้
df["district_id"] = df["district_id"].astype(str)
geo_df["CC_2"] = geo_df["CC_2"].astype(str)

# รวมตาราง df กับ geo_df เพื่อตรวจสอบการแมป
mapping_df = pd.merge(
    df,
    geo_df,
    how="outer",  # เอาทั้งสองฝั่งมาดูครบ
    left_on="district_id",
    right_on="CC_2",
    indicator=True  # จะมีคอลัมน์ _merge บอกว่ามาจากฝั่งไหน
)

# แสดงตัวอย่างผลลัพธ์
st.markdown("### 🔍 Mapping ตรวจสอบข้อมูลอำเภอ")
st.dataframe(mapping_df[["district_id", "district", "province", "CC_2", "NAME_2", "NAME_1", "_merge"]])


### แมพผิด 
# หนองจอก-หนองเข็ม? เลขสลับ
# ชื่อซ้ำ
# 1404 บางไทร - 1413 บางซ้าย # บางซ้ายหายไปเข้า 1404
# 1705 ท่าช้าง
# 1905 nong saeng
# 1913 เฉลิมพระเกียร
# พะเยา
# เรามี อำเภอ ชะอวด นครศรีธรรมราช เขาไม่มี