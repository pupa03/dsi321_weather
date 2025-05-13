import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import json

st.set_page_config(page_title="Choropleth Map", page_icon="🗺️")

st.title("แผนที่มลพิษรายอำเภอ (Choropleth)")


import os

BASE_DIR = os.getcwd()
geojson_path = os.path.join(BASE_DIR, "save", "gadm41_THA_2.json")

# โหลดไฟล์
@st.cache()
def load_data():
    with open(geojson_path, "r", encoding="utf-8") as f:
        district_geojson = json.load(f)


# 1. โหลด GeoJSON อำเภอ
# with open("../work/save/gadm41_THA_2.json", "r", encoding="utf-8") as f:
    # amphoe_geojson = json.load(f)

st.write(amphoe_geojson)
st.write(len(amphoe_geojson))

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

st.dataframe(df.head())



# 3. สร้างแผนที่ Folium
m = folium.Map(location=[13.5, 100.7], zoom_start=6)

# 4. สร้าง Choropleth map
folium.Choropleth(
    geo_data=amphoe_geojson, # ไฟล์ geojson
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
    amphoe_geojson,
    name="อำเภอ",
    tooltip=folium.GeoJsonTooltip(
        fields=["NAME_2"],
        aliases=["อำเภอ:"],
        localize=True
    )
).add_to(m)

# 5. แสดงแผนที่ใน Streamlit
st_folium(m, height=700)


# ดึงรหัสอำเภอทั้งหมดจาก GeoJSON
geo_ids = set([
    feature["properties"]["CC_2"]
    for feature in amphoe_geojson["features"]
])
st.write(len(geo_ids))
st.dataframe(geo_ids)

# ดึงรหัสอำเภอที่มีใน df (ค่าฝุ่น)
df_ids = set(df["district_id"].dropna().astype(int).astype(str))

st.dataframe(df_ids)
# หาอำเภอที่ไม่มีข้อมูล (อยู่ใน geo แต่ไม่อยู่ใน df)
missing_ids = geo_ids - df_ids
st.write(missing_ids)

# ดึงชื่ออำเภอที่ไม่มีข้อมูล
missing_names = [
    f"{feature['properties']['CC_2']} - {feature['properties']['NAME_1']} - {feature['properties']['NAME_2']}"
    for feature in amphoe_geojson["features"]
    if feature["properties"]["CC_2"] in missing_ids
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
    for feature in amphoe_geojson["features"]
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