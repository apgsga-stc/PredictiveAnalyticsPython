## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import altair as alt
from typing import Tuple

from pa_lib.data import select_rows, as_dtype
from .UpliftLib import all_weekdays, DataSeries, DataFrame


########################################################################################
# Helper Functions
########################################################################################
def heatmap_range(s: DataSeries, scale: float = 1) -> Tuple[float, float, float]:
    """Return min and max of a series, but makes range centered at 0 if it contains 0.
       Parameter scale [0..1] allows to tighten the range."""
    maximum = s.max()
    minimum = s.min()
    if maximum * minimum < 0:  # different sign
        middle = 0
        result_range = (min(-maximum, minimum), middle, max(-minimum, maximum))
    else:
        middle = (minimum + maximum) / 2
        result_range = (minimum, middle, maximum)
    if scale != 1:
        scaled = (result_range[2] - middle) * scale
        result_range = (middle - scaled, middle, middle + scaled)
    return result_range


def prepare_chart_data(data: DataFrame, selectors: dict) -> DataFrame:
    return (
        select_rows(data, selectors)
        .pipe(as_dtype, "int", incl_pattern="spr|.*pers$")
        .reset_index()
    )


########################################################################################
# Main Plotting Functions
########################################################################################
def heatmap(data: DataFrame, title: str, time_scale: str, properties: dict):
    color_range = ["darkred", "white", "darkgreen"]
    chart = (
        alt.Chart(data, title=title)
        .properties(**properties)
        .mark_rect()
        .encode(
            x="Station:N",
            y=f"{time_scale}:O",
            color=alt.Color(
                "pop_uplift_pers:Q",
                title="Uplift [Pers]",
                scale=alt.Scale(
                    range=color_range,
                    domain=heatmap_range(data["pop_uplift_pers"], scale=0.8),
                ),
            ),
            tooltip=[
                alt.Tooltip("Station", title="Bahnhof"),
                time_scale,
                alt.Tooltip("spr:Q", title="Total"),
                alt.Tooltip("target_pers", title="Zielgruppe"),
                alt.Tooltip("pop_uplift_pers:Q", title="Uplift"),
            ],
            row=alt.Row("DayOfWeek", title="Wochentag", sort=all_weekdays),
        )
        .resolve_scale(x="independent", color="independent")
    )
    return chart


def barplot(
    data: DataFrame, title: str, time_scale: str, axes: str, properties: dict
) -> alt.Chart:
    color_range = ["darkred", "white", "darkgreen"]
    chart = (
        alt.Chart(data, title=title)
        .properties(**properties)
        .mark_bar(stroke="black", strokeOpacity=0.5, strokeWidth=0.8)
        .encode(
            x=alt.X("pop_uplift_pers:Q", title=""),
            y=alt.Y(
                f"{time_scale}:O",
                axis=alt.Axis(grid=True),
                sort=data[time_scale].cat.categories.to_list(),
            ),
            tooltip=[
                time_scale,
                alt.Tooltip("spr:Q", title="Total"),
                alt.Tooltip("target_pers", title="Zielgruppe"),
                alt.Tooltip("pop_uplift_pers:Q", title="Uplift"),
            ],
            color=alt.Color(
                "pop_uplift_pers:Q",
                title="Uplift [Pers]",
                scale=alt.Scale(
                    range=color_range,
                    domain=heatmap_range(data["pop_uplift_pers"], scale=0.8),
                ),
            ),
            # color=alt.condition(
            #     alt.datum.pop_uplift_pers > 0,
            #     alt.value("green"),  # The positive color
            #     alt.value("red"),  # The negative color
            # ),
            row=alt.Row("DayOfWeek", title="Wochentag", sort=all_weekdays),
            column=alt.Column("Station", title="Bahnhof"),
        )
        .resolve_scale(x=axes)
    )
    return chart
