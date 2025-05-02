import streamlit as st
import leafmap.foliumap as leafmap
import pandas as pd
import pyarrow
import folium
from streamlit_folium import st_folium

st.set_page_config(page_title="Heatmap", page_icon="🔥")
# st.set_page_config(layout="wide")

markdown = """
A Streamlit map template
<https://github.com/opengeos/streamlit-map-template>
"""

st.sidebar.title("About")
st.sidebar.info(markdown)
logo = "https://i.imgur.com/UbOXYAU.png"
st.sidebar.image(logo)

st.title("Heatmap")


df = pd.read_parquet("save/f65bb697be7843fd9e092d83f914065f-0.parquet", engine="pyarrow")

# st.write(df.head())

# min_val, max_val = 0, 100
# df["weight"] = df["pm2.5"].clip(lower=min_val, upper=max_val)
df["weight"] = df["pm2.5"].apply(lambda x: x if x > 50 else 0)
st.write(df.head())
# df["weight"] = (df["weight"] - min_val) / (max_val - min_val)


with st.expander("See source code"):
    with st.echo():
        # filepath = "https://raw.githubusercontent.com/giswqs/leafmap/master/examples/data/us_cities.csv"
        center = [df["lat"].mean(), df["lon"].mean()]
        m = leafmap.Map(center=center, zoom=5)
        # m = leafmap.Map(zoom=4)
        m.add_heatmap(
            data=df,
            latitude="lat",
            longitude="lon",
            value="weight",
            name="Heat map",
            radius=16
        )


m.to_streamlit(height=1200)
