Gathers data from storm events data from NOAA and uses mini-batch k-means clustering to generate labels to rank event severity. This severity metric is then used to generate a choropleth map highlighting areas of the country in which bad weather occurs more often and/or is more severe by summing the scores of events, grouped by county, and scaled by county area.

While the damages, deaths, and injuries features were scaled based on county population and the sum of event scores have been scaled by county area, there still seems to be some correlation with population density in the final county severity measure, particularly in areas of very low population density. This may simply be due to the goal of the data source: to report weather events with significant loss of life, damage of property, or disruption of commerce. Thus, it is likely some severe weather events have gone unreported in the original data simply due to the lack of impact on human life.

![](https://github.com/VioletteVanadium/severe_weather_history_chloropleth/blob/master/map.png)

To use: run <code>./main.sh</code>.
