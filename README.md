Gathers data from storm events data from NOAA and uses mini-batch k-means clustering to generate labels to rank event severity. This severity metric is then used to generate a chloropleth map highlighting areas of the country in which bad weather occurs more often and/or is more severe.

TODO: Scale Damage, Death, and Injury features by population

![Normalized by county area](https://github.com/VioletteVanadium/severe_weather_history_chloropleth/blob/master/map.png)

To use: run <code>./main.sh</code>.
