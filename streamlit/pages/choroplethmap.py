import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import json

st.set_page_config(page_title="Choropleth Map", page_icon="🗺️")

st.title("แผนที่มลพิษรายอำเภอ (Choropleth)")


import os

BASE_DIR = os.getcwd()
# BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
st.write(BASE_DIR)
geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")
st.write(geojson_path)

# กำหนด path แบบ relative จากตำแหน่งของไฟล์ Python นี้
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# geojson_path = os.path.join(BASE_DIR, "../save/gadm41_THA_2.json")

# # โหลดไฟล์
with open(geojson_path, "r", encoding="utf-8") as f:
     amphoe_geojson = json.load(f)


# 1. โหลด GeoJSON อำเภอ
# with open("../work/save/gadm41_THA_2.json", "r", encoding="utf-8") as f:
    # amphoe_geojson = json.load(f)

st.write(amphoe_geojson)
st.write(len(amphoe_geojson))

# 2. โหลดข้อมูลค่ามลพิษ (เช่น pm2.5) รายอำเภอ
# ต้องมีคอลัมน์: "amphoe_code" (หรือรหัสอำเภอที่ตรงกับ GeoJSON) และ "pm25"
# df = pd.read_parquet("save/f65bb697be7843fd9e092d83f914065f-0.parquet", engine="pyarrow")
df = pd.read_csv("save/f20c8d74ef9641aca1f97d8b39261aa4-0.csv")
df_code = pd.read_csv("save/amphoe_coord.csv")

# แสดงตัวอย่างข้อมูล

df_code = df_code.rename(columns={"amphoeEN":"district"})
st.dataframe(df.head())
st.dataframe(df_code.head())

df = pd.merge(
    df,
    df_code[["district", "amphoe_id"]],
    on="districts",
    how="left"  # ใช้ 'left' เพื่อคงข้อมูล df หลักไว้ทั้งหมด
)
st.dataframe(df.head())



# 3. สร้างแผนที่ Folium
m = folium.Map(location=[13.5, 100.7], zoom_start=6)

# 4. สร้าง Choropleth map
folium.Choropleth(
    geo_data=amphoe_geojson,
    data=df,
    columns=["amphoe_id", "pm2.5"],  # รหัสอำเภอ และค่า
    key_on="feature.properties.CC_2",  # เปลี่ยนตามโครงสร้าง GeoJSON
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="PM2.5 (µg/m³)",
).add_to(m)

# Optional: ใส่ popup ชื่ออำเภอ
folium.GeoJson(
    amphoe_geojson,
    name="อำเภอ",
    tooltip=folium.GeoJsonTooltip(
        fields=["NAME_2"],
        aliases=["อำเภอ:"],
        localize=True
    )
).add_to(m)

# 5. แสดงแผนที่ใน Streamlit
st_folium(m, height=1600)





# ดึงรหัสอำเภอทั้งหมดจาก GeoJSON
geo_ids = set([
    feature["properties"]["CC_2"]
    for feature in amphoe_geojson["features"]
])
st.write(len(geo_ids))
st.dataframe(geo_ids)
# ดึงรหัสอำเภอที่มีใน df (ค่าฝุ่น)
df_ids = set(df["amphoe_id"].dropna().astype(int).astype(str))

st.dataframe(df_ids)
# หาอำเภอที่ไม่มีข้อมูล (อยู่ใน geo แต่ไม่อยู่ใน df)
missing_ids = geo_ids - df_ids

# ดึงชื่ออำเภอที่ไม่มีข้อมูล
missing_names = [
    feature["properties"]["NAME_2"]
    for feature in amphoe_geojson["features"]
    if feature["properties"]["CC_2"] in missing_ids
]

# แสดงผล
st.markdown(f"### ❌ อำเภอที่ไม่มีค่าฝุ่น (pm2.5): {len(missing_names)} รายการ")
st.write(missing_names)
