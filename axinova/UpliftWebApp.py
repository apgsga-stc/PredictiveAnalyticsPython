## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))


import streamlit as st
import os
import socket
import urllib.parse

from UpliftTarget import source_data, Target
from pa_lib.const import PA_DATA_DIR


# Get environment (different if we're running in a container)
def get_run_environment() -> tuple:
    host_name = os.getenv("ZG_HOST_NAME", socket.getfqdn())
    host_port = os.getenv("ZG_EXPORT_PORT", 8081)
    export_dir = os.getenv("ZG_EXPORT_DIR", f"{PA_DATA_DIR}/axinova/zielgruppen_export")
    return host_name, host_port, export_dir


def choose_target(all_targets) -> str:
    return st.selectbox(
        label="Choose Target Group:",
        options=list(all_targets.keys()),
        index=0,
        format_func=lambda key: all_targets[key].name,
    )


@st.cache(allow_output_mutation=True)
def calculate_target(target: Target) -> Target:
    target.set_timescale("Hour")
    target.calculate()
    return target


def download_results(target: Target) -> None:
    if st.button("Create XLSX file with all results"):
        host_name, host_port, export_dir = get_run_environment()
        export_file_name = target.export_result(to_directory=export_dir)
        html_file_name = urllib.parse.quote(export_file_name.encode("utf-8"))
        file_address = f"http://{host_name}:{host_port}/{html_file_name}"
        st.markdown(
            f'Download: <a href="{file_address}">{export_file_name}</a>',
            unsafe_allow_html=True,
        )


def choose_stations() -> list:
    st.sidebar.markdown("### Table / Plot Parameters")
    return st.sidebar.multiselect(
        label="Choose stations:", options=source_data.all_stations
    )


def describe_target(target: Target) -> None:
    st.markdown("# Zielgruppen-Analyse")
    st.markdown(f"## Target Group: {target.name}")
    st.markdown("### Definition:")
    st.text(target.description())


def show_summary(target: Target) -> None:
    st.markdown("### Summary:")
    st.table(target.result_summary())
    st.markdown("### All stations (sum per week):")
    station_table = target.best_stations(where="spr > 0")
    st.table(station_table)


def show_station_weekdays(target: Target, station_list: list) -> None:
    if station_list == list():
        st.warning("Choose at least one station!")
        return
    st.markdown(f"### Stationen pro Wochentag für {', '.join(sorted(station_list))}:")
    st.table(
        target.best_station_days(
            where="Station == ['" + "', '".join(station_list) + "']"
        )[["spr", "target_ratio", "target_pers", "target_error_prc"]]
    )


def show_station_heatmaps(target: Target, station_list: list) -> None:
    if station_list == list():
        st.warning("Choose at least one station!")
        return
    st.markdown(f"### Heatmaps für {', '.join(sorted(station_list))}:")
    show_uncertainty = st.checkbox("Unsicherheit anzeigen")
    heatmaps = target.plot_station_heatmaps(
        selectors={"Station": station_list}, show_uncertainty=show_uncertainty
    )
    st.altair_chart(heatmaps, use_container_width=True)


def download_station_heatmaps(target: Target, station_list: list) -> None:
    if station_list == list():
        st.warning("Choose at least one station!")
        return
    show_uncertainty = st.checkbox("Unsicherheit anzeigen")
    if st.button("Create ZIP file with all plots"):
        host_name, host_port, export_dir = get_run_environment()
        plot_file_name = target.store_station_heatmaps(
            show_uncertainty=show_uncertainty,
            directory=export_dir,
            selectors={"Station": station_list},
        )
        html_file_name = urllib.parse.quote(plot_file_name.encode("utf-8"))
        file_address = f"http://{host_name}:{host_port}/{html_file_name}"
        st.markdown(
            f'Download: <a href="{file_address}">{plot_file_name}</a>',
            unsafe_allow_html=True,
        )


def show_timeslots(target: Target) -> None:
    st.sidebar.markdown("### Best Slot Parameters")
    sort_col_list = st.sidebar.multiselect(
        label="Sort by column(s) [descending]:",
        options=list(target.result.columns),
        default="pop_uplift_ratio target_pers".split(),
    )
    show_col_list = st.sidebar.multiselect(
        label="Show columns:",
        options=list(target.result.columns),
        default=[
            "spr",
            "target_ratio",
            "target_pers",
            "pop_uplift_ratio",
            "pop_uplift_pers",
            "target_error_prc",
        ],
    )
    target_where = st.sidebar.text_area(
        "Filter for best slots:", value="target_error_prc < 10 and target_pers > 360"
    ).replace("\n", " ")
    nr_best_slots = st.sidebar.number_input(
        "Number of best slots:", min_value=1, max_value=500, value=20, format="%d"
    )
    st.markdown(f"### Best Slots by Ratio:")
    st.markdown(f"_**Filter:** {target_where}_")
    st.table(
        target.best_slots(
            column=sort_col_list,
            where=target_where,
            top_n=nr_best_slots,
            show_col=show_col_list,
        )
    )


def show_timeslot_plots(target: Target, station_list: list) -> None:
    if station_list == list():
        st.warning("Choose at least one station!")
        return
    st.markdown(f"### Plots für {', '.join(sorted(station_list))}:")
    barplot = target.plot_timeslot_plots(selectors={"Station": station_list})
    st.altair_chart(barplot, use_container_width=False)


def download_timeslot_plots(target: Target, station_list: list) -> None:
    if station_list == list():
        st.warning("Choose at least one station!")
        return
    if st.button("Create ZIP file with all plots"):
        host_name, host_port, export_dir = get_run_environment()
        plot_file_name = target.store_timeslot_plots(
            directory=export_dir, selectors={"Station": station_list}
        )
        html_file_name = urllib.parse.quote(plot_file_name.encode("utf-8"))
        file_address = f"http://{host_name}:{host_port}/{html_file_name}"
        st.markdown(
            f'Download: <a href="{file_address}">{plot_file_name}</a>',
            unsafe_allow_html=True,
        )
