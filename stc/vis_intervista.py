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

from prep_intervista import master_intervista_loader

########################################################################################
(mobility_cat_dist_stacked, mobility_dist_mean_med_stacked) = master_intervista_loader()


########################################################################################
## Plot Percentages by Distance- and Radius categories:

subset_df = mobility_cat_dist_stacked
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
del subset_df

########################################################################################


subset_df = mobility_dist_mean_med_stacked

fig2 = px.line(
    subset_df,
    x="Datum",
    y="kilometer",
    facet_col="social_demographic",
    facet_row="Beschreibung",
    color="Typ",
    # mode='lines+markers',
    # trendline="lowess",
    # range_x=[47,85],
)

fig2.show(renderer="browser")
del subset_df

########################################################################################
