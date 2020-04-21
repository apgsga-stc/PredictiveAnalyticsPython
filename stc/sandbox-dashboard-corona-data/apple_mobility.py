#!/usr/bin/env python
# coding: utf-8

# In[13]:


# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
print(file_dir)
parent_dir = file_dir.parent
print(parent_dir)
sys.path.append(str(parent_dir))

data_dir = Path.home() / "data" / "dashboard-corona"


import plotly.io as pio

pio.renderers.default = "browser"

from datetime import datetime
import pandas as pd
import plotly.graph_objs as go

# # apple data

# https://www.apple.com/covid19/mobility


########################################################################################
link_apple = data_dir / "applemobilitytrends-2020-04-13.csv"
apple_mobility = pd.read_csv(link_apple, low_memory=False)
apple_mobility.loc[:, "data_prep_date"] = datetime.now()  # Technical Timestamp
########################################################################################
data_value_columns = [x for x in apple_mobility.columns if "2020" in x]
keys_columns = [x for x in apple_mobility.columns if x not in data_value_columns]
apple_mobility_melted = pd.melt(
    apple_mobility,
    id_vars=keys_columns,
    value_vars=data_value_columns,
    var_name="date",
).astype({"date": "datetime64[ns]"})
########################################################################################
apple_mobility_ch = apple_mobility_melted[
    apple_mobility_melted.region.isin(["Zurich", "Switzerland"])
].reset_index().drop(columns=["index"])
########################################################################################
apple_mobility_ch.to_feather(str(data_dir / "apple_mobility.feather"))
########################################################################################

apple_mobility_data = pd.read_feather(data_dir / "apple_mobility_data.feather")

plot_data = apple_mobility_data[apple_mobility_data.region == "Switzerland"]


fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=[plot_data.date.min(),plot_data.date.max()],
        y= [100,100],
        hoverinfo="skip",
        mode="lines",
        name="baseline",
    )
)
for (transportation_type, subset_df) in plot_data.groupby("transportation_type"):
    fig.add_trace(
        go.Scatter(
            x=subset_df.date,
            y=subset_df.percent,
            name=transportation_type,
            mode="lines",
            line_shape="spline",  # linear",
            line=dict(color="white", width=13),
            opacity=0.5,
            showlegend=False,
            hoverinfo="skip",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=subset_df.date,
            y=subset_df.percent,
            name=transportation_type,
            mode="markers",
            line_shape="spline",  # linear",
            # line=dict(color=color, width=2),
            hovertemplate=f"<b>{transportation_type}</b>"
            + "<br><i>Prozent:</i> <b>%{y:.2f}%</b>"
            + "<br><i>Datum:</i> <b>%{text}</b><br>"
            + "<extra></extra>",
            text=subset_df.date.dt.strftime("%a, %d. %b (KW %V)"),
        )
    )

fig.show(renderer="browser")
