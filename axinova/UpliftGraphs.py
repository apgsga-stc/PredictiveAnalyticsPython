## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path


file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import streamlit as st
import altair as alt
from typing import Tuple, List, Dict, Union
from zipfile import ZipFile
from datetime import datetime as dt

from pa_lib.data import select_rows, as_dtype, unfactorize
from pa_lib.file import make_file_name
from pa_lib.util import chunkify
from .UpliftLib import all_weekdays, DataSeries, DataFrame


########################################################################################
# Helper Functions
########################################################################################
def heatmap_range(s: DataSeries, scale: float = 1.0) -> Tuple[float, float, float]:
    """Return min, middle, and max of a series, centering range at 0 if it contains 0.
       Parameter scale allows to tighten/widen the range."""
    maximum = float(s.max())
    minimum = float(s.min())
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


def store_charts_to_files(charts: Dict[str, alt.Chart], to_directory: str) -> List[str]:
    chart_files: List[str] = list()
    num_charts = len(charts)
    charts_done = 0
    st.write(f"Creating {num_charts} plots:")
    show_bar = st.progress(0)
    for (label, chart) in charts.items():
        chart_file_name = make_file_name(label) + ".png"
        chart_file_path = Path(to_directory) / chart_file_name
        chart.properties(background="white").save(str(chart_file_path), format="png")
        charts_done += 1
        show_bar.progress(charts_done / num_charts)
        chart_files.append(chart_file_name)
    return chart_files


def make_zip_file(
    file_names: List[str], from_directory: str, to_directory: str, archive_name: str
) -> str:
    zip_file_name = (
        f"{make_file_name(archive_name)}_{dt.now().strftime('%Y%m%d_%H%M%S')}.zip"
    )
    zip_file_path = Path(to_directory) / zip_file_name
    with ZipFile(zip_file_path, "w") as archive:
        for file_name in file_names:
            file_path = Path(from_directory) / file_name
            archive.write(file_path, arcname=file_name)
            file_path.unlink()
    return zip_file_name


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
        color_range = ["darkred", "#FEFEFE", "darkgreen"]
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


def barplots(
    data: DataFrame,
    selectors: dict,
    timescale: str,
    target_col: str,
    target_threshold: float,
    properties: dict,
    result_type: str = "chart",
    directory: str = None,
    archive_name: str = None,
) -> Union[alt.Chart, str]:
    chart_data = prepare_chart_data(data, selectors)
    if chart_data.shape[0] == 0:
        st.info("Choose at least one station in the sidebar")
        return ""
    y_scale_sort = data.reset_index()[timescale].cat.categories.to_list()

    def station_weekday_barplot(station: str, weekday: str,) -> alt.Chart:
        single_chart_data = chart_data.loc[
            (chart_data["Station"] == station) & (chart_data["DayOfWeek"] == weekday)
        ]
        chart = (
            alt.Chart(single_chart_data, title=f"Uplift [persons] {station}: {weekday}")
            .properties(**properties)
            .mark_bar()
            .encode(
                x=alt.X(f"{target_col}:Q", title=""),
                y=alt.Y(f"{timescale}:O", axis=alt.Axis(grid=True), sort=y_scale_sort),
                color=alt.condition(
                    alt.datum[target_col] > target_threshold,
                    alt.value("darkgreen"),  # The positive color
                    alt.value("darkred"),  # The negative color
                ),
            )
        )
        return chart

    charts = {
        f"{station}_{weekday}": station_weekday_barplot(station, weekday)
        for station in sorted(chart_data["Station"].unique())
        for weekday in all_weekdays
    }
    if result_type == "chart":
        chart_matrix = alt.hconcat(
            *(
                alt.vconcat(*chart_row)
                .properties(spacing=50)
                .resolve_scale(color="independent")
                for chart_row in chunkify(list(charts.values()), 7)
            )
        ).properties(
            background="white",
            spacing=50,
            padding=dict(top=20, left=20, right=20, bottom=60),
        )
        return chart_matrix
    elif result_type == "zip":
        chart_file_names = store_charts_to_files(charts, to_directory=directory)
        zip_file_name = make_zip_file(
            chart_file_names,
            from_directory=directory,
            to_directory=directory,
            archive_name=archive_name,
        )
        return zip_file_name
    else:
        raise ValueError(
            f"Parameter result_type ('{result_type}') not in ['chart', 'zip']"
        )


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
        color_range = ["#FEFEFE", "darkgreen"]
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
    data: DataFrame,
    selectors: dict,
    properties: dict,
    show_uncertainty: bool = False,
    result_type: str = "chart",
    directory: str = None,
    archive_name: str = None,
) -> Union[alt.Chart, str]:
    chart_data = prepare_chart_data(data, selectors)
    chart_data.loc[:, "target_error_txt"] = (
        chart_data["target_error_prc"].astype("str") + "%"
    )

    def station_heatmap(station: str) -> alt.Chart:
        single_chart_data = chart_data.query("Station == @station")
        single_heatmap = (
            alt.Chart(single_chart_data, title=station)
            .mark_rect()
            .encode(
                x=alt.X("DayOfWeek:O", sort=all_weekdays),
                y=alt.Y("Hour:O"),
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
                tooltip=[
                    alt.Tooltip("Station", title="Bahnhof"),
                    alt.Tooltip("DayOfWeek", title="Wochentag"),
                    alt.Tooltip("Hour", title="Stunde"),
                    alt.Tooltip("spr:Q", title="Total"),
                    alt.Tooltip("target_pers", title="Zielgruppe"),
                    alt.Tooltip("target_error_txt", title="Unsicherheit"),
                ],
            )
        )
        if show_uncertainty:
            error_marks = single_heatmap.mark_circle(size=50).encode(
                color=alt.Color(
                    "target_error_prc:Q",
                    title="Unsicherheit [%]",
                    scale=alt.Scale(
                        range=["darkgreen", "yellow", "red"], domain=[0, 100]
                    ),
                )
            )
            single_heatmap += error_marks
        return single_heatmap.properties(**properties).resolve_scale(
            color="independent"
        )

    charts = {
        station: station_heatmap(station)
        for station in sorted(chart_data["Station"].unique())
    }
    if result_type == "chart":
        chart_matrix = alt.vconcat(
            *(
                alt.hconcat(*chart_row)
                .properties(spacing=30)
                .resolve_scale(color="independent")
                for chart_row in chunkify(list(charts.values()), 3)
            )
        ).properties(
            background="white",
            spacing=100,
            padding=dict(top=20, left=20, right=20, bottom=60),
        )
        return chart_matrix
    elif result_type == "zip":
        chart_file_names = store_charts_to_files(charts, to_directory=directory)
        zip_file_name = make_zip_file(
            chart_file_names,
            from_directory=directory,
            to_directory=directory,
            archive_name=archive_name,
        )
        return zip_file_name
    else:
        raise ValueError(
            f"Parameter result_type ('{result_type}') not in ['chart', 'zip']"
        )
