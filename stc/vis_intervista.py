# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
print(file_dir)
parent_dir = file_dir.parent
print(parent_dir)
sys.path.append(str(parent_dir))

# from pa_lib.job import request_job
# from pa_lib.file import project_dir, load_bin, load_csv
# from pa_lib.data import desc_col

# from pa_lib.vis import boxplot_histogram
import plotly.io as pio

pio.renderers.default = "browser"
import plotly.express as px

from prep_intervista import master_intervista_loader, self_isolation_mapper
import pandas as pd
import numpy as np
import plotly.graph_objects as go

########################################################################################
(mobility_cat_dist_stacked, mobility_dist_mean_med_stacked) = master_intervista_loader()


########################################################################################
## Plot Percentages by Radius categories:

subset_df = mobility_cat_dist_stacked[
    (
        mobility_cat_dist_stacked.social_demographic.isin(["Total"])
        & (mobility_cat_dist_stacked.Beschreibung.isin(["Radius"]))
    )
]
self_isolation = subset_df.Ausprägung.apply(lambda x: self_isolation_mapper(x)).copy()
subset_df.loc[:, "quarantine"] = self_isolation
subset_df2 = (
    subset_df.groupby(
        ["Datum", "Beschreibung", "dayofyear", "social_demographic", "quarantine"]
    )
    .agg({"percent": "sum"})
    .reset_index()
    .copy()
)
# Drawing lines:

fig = go.Figure()
for (quarantine_x, plot_data) in subset_df2.groupby("quarantine"):
    fig.add_trace(
        go.Scatter(
            x=plot_data.Datum,
            y=plot_data.percent,
            name=quarantine_x,
            mode="lines+markers",
            line_shape="spline",  #'linear'
            hovertemplate=f"<b>{quarantine_x}</b><br>"
            + "<br><b>Prozent:</b> %{y:.2f}%"
            + "<br><b>Datum:</b> %{text}<br>"
            + "<extra></extra>",
            text=plot_data.Datum.dt.strftime("%A, %d. %B"),
        )
    )
fig.update_layout(
    yaxis=dict(range=[-5, 100],),
    title="Mobility-Radius: Wieviel Prozent der Bevölkerung bleibt zuhaus? (Intervista)",
    xaxis_title="Datum",  # "Zeit",
    yaxis_title="Prozent",
    #plot_bgcolor= 'rgba(0,0,0,0)',
    shapes=[
        dict(
            type="line",
            yref="paper",
            y0=0,
            y1=1,
            xref="x",
            x0=pd.to_datetime("13.03.2020", format="%d.%m.%Y"),
            x1=pd.to_datetime("13.03.2020", format="%d.%m.%Y"),
            opacity=0.2
        )
    ],
)
fig.show(renderer="browser")

# Second drawing
fig = px.bar(
    subset_df,
    x="Datum",
    y="percent",
    facet_col="social_demographic",
    facet_row="Beschreibung",
    color="Ausprägung",
)
fig.update_layout(barmode="stack")
fig.show(renderer="browser")

del subset_df2, subset_df

########################################################################################
## Travelling bubbles

subset_df = mobility_cat_dist_stacked[
    (
        mobility_cat_dist_stacked.social_demographic.isin(["Total"])
        & (mobility_cat_dist_stacked.Beschreibung.isin(["Radius"]))
    )
]
fig = px.scatter(
    subset_df,
    x="Datum",
    y="percent",
    animation_frame="dayofyear",
    animation_group="Ausprägung",
    # facet_col="social_demographic",
    # facet_row="Beschreibung",
    color="Ausprägung",
    size=np.log(subset_df.percent + 1),
    range_x=[
        pd.to_datetime("15.02.2020", format="%d.%m.%Y"),
        pd.to_datetime("30.03.2020", format="%d.%m.%Y"),
    ],
    range_y=[0, 50],
)
fig.show(renderer="browser")

########################################################################################

subset_df = mobility_dist_mean_med_stacked[
    mobility_dist_mean_med_stacked.social_demographic.isin(["Total"])
    & mobility_dist_mean_med_stacked.Beschreibung.isin(["Distanz"])
]
fig = go.Figure()

for (median_mean_typ, plot_data) in subset_df.groupby("Typ"):
    fig.add_trace(
        go.Scatter(
            x=plot_data.Datum,
            y=plot_data.kilometer,
            name=median_mean_typ,
            mode="lines+markers",
            line_shape="spline",
        )
    )
fig.show(renderer="browser")


########################################################################################
