## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import altair as alt
from typing import Tuple, List

from pa_lib.data import select_rows, as_dtype, unfactorize
from .UpliftLib import all_weekdays, DataSeries, DataFrame


########################################################################################
# Helper Functions
########################################################################################
def heatmap_range(s: DataSeries, scale: float = 1.0) -> Tuple[float, float, float]:
    """Return min, middle, and max of a series, centering range at 0 if it contains 0.
       Parameter scale allows to tighten/widen the range."""
    maximum = s.max().astype("float")
    minimum = s.min().astype("float")
    if maximum * minimum < 0.0:  # different sign --> 0 is in range
        middle = 0.0
        result_range = (min(-maximum, minimum), middle, max(-minimum, maximum))
        if scale != 1.0:
            scaled = (result_range[2] - middle) * scale
            result_range = (middle - scaled, middle, middle + scaled)
    else:
        is_positive = minimum >= 0
        middle = (minimum + maximum) / 2
        result_range = (minimum, middle, maximum)
        if scale != 1.0:
            if is_positive:
                result_range = (minimum, middle * scale, maximum * scale)
            else:
                result_range = (minimum * scale, middle * scale, maximum)
    return result_range


def prepare_chart_data(data: DataFrame, selectors: dict) -> DataFrame:
    return (
        select_rows(data, selectors)
        .pipe(as_dtype, "int", incl_pattern="spr|.*pers$")
        .reset_index()
        .pipe(unfactorize)
    )


########################################################################################
# Main Plotting Functions
########################################################################################
def heatmap(
    data: DataFrame,
    selectors: dict,
    title: str,
    timescale: str,
    target_col: str,
    target_title: str,
    properties: dict,
    color_range: List[str] = None,
):
    if color_range is None:
        color_range = ["darkred", "white", "darkgreen"]
    chart_data = prepare_chart_data(data, selectors)
    chart = (
        alt.Chart(chart_data, title=title)
        .properties(**properties)
        .mark_rect()
        .encode(
            x="Station:N",
            y=f"{timescale}:O",
            color=alt.Color(
                f"{target_col}:Q",
                title=target_title,
                scale=alt.Scale(
                    range=color_range,
                    type="linear",
                    zero=True,
                    domain=heatmap_range(chart_data[target_col], scale=0.8),
                ),
            ),
            tooltip=[
                alt.Tooltip("Station", title="Bahnhof"),
                timescale,
                alt.Tooltip("spr:Q", title="Total"),
                alt.Tooltip("target_pers", title="Zielgruppe"),
                alt.Tooltip(f"{target_col}:Q", title=target_title),
            ],
            row=alt.Row("DayOfWeek", title="Wochentag", sort=all_weekdays),
        )
        .resolve_scale(x="independent")
    )
    return chart


def barplot(
    data: DataFrame,
    selectors: dict,
    title: str,
    timescale: str,
    target_col: str,
    target_threshold: float,
    target_title: str,
    axes: str,
    properties: dict,
) -> alt.Chart:
    chart_data = prepare_chart_data(data, selectors)
    chart_data["error"] = (chart_data["target_pers_sd_ratio"] * 100 + 0.5).astype("int")
    plot = (
        alt.Chart(chart_data, title=title)
        .properties(**properties)
        .mark_bar()
        .encode(
            x=alt.X(f"{target_col}:Q", title=""),
            y=alt.Y(
                f"{timescale}:O",
                axis=alt.Axis(grid=True),
                sort=data.reset_index()[timescale].cat.categories.to_list(),
            ),
            tooltip=[
                timescale,
                alt.Tooltip("spr:Q", title="Total"),
                alt.Tooltip("target_pers", title="Zielgruppe"),
                alt.Tooltip("error", title="Fehler [%]"),
                alt.Tooltip(f"{target_col}:Q", title=target_title),
            ],
            color=alt.condition(
                alt.datum[target_col] > target_threshold,
                alt.value("darkgreen"),  # The positive color
                alt.value("darkred"),  # The negative color
            ),
            row=alt.Row("DayOfWeek", title="Wochentag", sort=all_weekdays),
            column=alt.Column("Station", title="Bahnhof"),
        )
        .resolve_scale(x=axes, y=axes)  # "shared" | "independent"
    )
    return plot


def stations_weekday_heatmap(
    data: DataFrame,
    selectors: dict,
    title: str,
    target_col: str,
    target_title: str,
    properties: dict,
    color_range: List[str] = None,
):
    if color_range is None:
        color_range = ["white", "darkgreen"]
    chart_data = prepare_chart_data(data, selectors)
    chart = (
        alt.Chart(chart_data, title=title)
        .properties(**properties)
        .mark_rect()
        .encode(
            y="Station:N",
            x=alt.X("DayOfWeek:O", sort=all_weekdays),
            color=alt.Color(
                f"{target_col}:Q",
                title=target_title,
                aggregate="mean",
                scale=alt.Scale(
                    range=color_range,
                    type="linear",
                    zero=True,
                    domain=heatmap_range(chart_data[target_col], scale=0.8),
                ),
            ),
            tooltip=[
                alt.Tooltip("Station", title="Bahnhof"),
                alt.Tooltip("spr:Q", title="Total"),
                alt.Tooltip("target_pers", title="Zielgruppe"),
                alt.Tooltip(f"{target_col}:Q", title=target_title),
            ],
        )
    )
    return chart


def station_heatmaps(
    data: DataFrame, selectors: dict, properties: dict, show_uncertainty: bool = False
) -> alt.Chart:
    chart_data = prepare_chart_data(data, selectors)

    def station_heatmap(station: str) -> alt.Chart:
        single_chart_data = chart_data.query("Station == @station")
        single_chart_data.loc[:, "target_error_txt"] = (
            single_chart_data["target_error_prc"].astype("str") + "%"
        )
        base_encodings = dict(
            x=alt.X("DayOfWeek:O", sort=all_weekdays), y=alt.Y("Hour:O")
        )
        base_tooltips = [
            alt.Tooltip("Station", title="Bahnhof"),
            alt.Tooltip("DayOfWeek", title="Wochentag"),
            alt.Tooltip("Hour", title="Stunde"),
            alt.Tooltip("spr:Q", title="Total"),
            alt.Tooltip("target_pers", title="Zielgruppe"),
            alt.Tooltip("target_error_txt", title="Unsicherheit"),
        ]
        base_heatmap = (
            alt.Chart(single_chart_data, title=station)
            .mark_rect()
            .encode(
                **base_encodings,
                color=alt.Color(
                    "target_pers:Q",
                    title="Zielpersonen",
                    scale=alt.Scale(
                        # near-white ("white" has display problems) to dark green
                        range=["#FEFEFE", "DarkGreen"],
                        domain=heatmap_range(
                            single_chart_data["target_pers"], scale=1.1
                        ),
                    ),
                ),
                tooltip=base_tooltips,
            )
        )
        if show_uncertainty:
            error_marks = (
                alt.Chart(single_chart_data)
                .mark_circle(size=50)
                .encode(
                    **base_encodings,
                    color=alt.Color(
                        "target_error_prc:Q",
                        title="Unsicherheit [%]",
                        scale=alt.Scale(
                            range=["darkgreen", "yellow", "red"], domain=[0, 100]
                        ),
                    ),
                    tooltip=base_tooltips,
                )
            )
            base_heatmap += error_marks
        return base_heatmap.properties(**properties).resolve_scale(color="independent")

    charts = [
        station_heatmap(station) for station in sorted(chart_data["Station"].unique())
    ]
    return (
        alt.hconcat(*charts)
        .properties(background="white")
        .resolve_scale(color="independent")
    )
