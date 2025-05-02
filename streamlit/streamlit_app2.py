import streamlit as st
import pandas as pd
import numpy as np

st.title("🎈 My new app")
st.write('Test')


# df = pd.DataFrame(
#     np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
#     columns=["lat", "lon"],
# )

provinces = {
    "Pathum Thani": {"lat": 14.0134, "lon": 100.5304},
    "Bangkok": {"lat": 13.7367, "lon": 100.5232},
    "Chiang Mai": {"lat": 18.7883, "lon": 98.9853},
    "Phuket": {"lat": 7.9519, "lon": 98.3381}
}

# แปลง dict เป็น DataFrame
selected_province = st.selectbox("เลือกจังหวัด", list(provinces.keys()))
coord = provinces[selected_province]

df = pd.DataFrame.from_dict(provinces, orient='index').reset_index()
df.columns = ['province', 'lat', 'lon']

# แสดงแผนที่
st.map(df)

# df = pd.DataFrame([coord])
# st.map(df)
