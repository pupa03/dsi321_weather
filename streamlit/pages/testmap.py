fig = px.choropleth_mapbox(
    map_df,
    geojson=geojson,
    locations=locations,
    featureidkey=featureidkey,
    color="pm25",
    color_continuous_scale="YlOrRd",
    mapbox_style="carto-positron",
    zoom=5,
    center={"lat": 13.5, "lon": 100.5},
    opacity=0.6,
    labels={"pm25": "PM2.5"},
    hover_name=locations
)
st.plotly_chart(fig, use_container_width=True)