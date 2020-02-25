import streamlit as st
from UpliftTarget import source_data


@st.cache(allow_output_mutation=True)
def calculate_target(target):
    target.set_timescale("Hour")
    target.calculate()
    return target


def describe_target(target) -> None:
    st.markdown("# Zielgruppen-Analyse")
    st.markdown(f"## Target Group: {target.name}")
    st.markdown("### Definition:")
    st.text(target.description())


def show_results(target) -> None:
    st.markdown("### Summary:")
    st.table(target.result_summary())
    st.markdown("### Stations per week:")
    st.table(target.best_stations(where="spr > 0"))

    st.sidebar.markdown("### Station / Weekday Parameters")
    station_list = st.sidebar.multiselect(
        label="Show Stations:", options=source_data.all_stations, default=["Zürich HB"]
    )
    st.markdown("### Stations per weekday:")
    st.table(
        target.best_station_days(
            where="Station == ['" + "', '".join(station_list) + "']"
        )[["spr", "target_ratio", "target_pers", "target_pers_sd_ratio"]]
    )

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
            "target_pers_sd_ratio",
        ],
    )
    target_where = st.sidebar.text_area(
        "Filter(s) for Best Slots:",
        value="target_pers_sd_ratio < 0.1 and target_pers > 360",
    ).replace("\n", " ")
    nr_best_slots = st.sidebar.number_input(
        "Number of Best Slots:", min_value=1, max_value=500, value=20, format="%d"
    )
    st.markdown(f"### Best Slots by Ratio:")
    st.table(
        target.best_slots(
            column=sort_col_list,
            where=target_where,
            top_n=nr_best_slots,
            show_col=show_col_list,
        )
    )


def show_plots(target) -> None:
    st.sidebar.markdown("### Plot Parameters")
    station_list = st.sidebar.multiselect(
        label="Show Stations:",
        options=source_data.all_stations,
        default=["Basel SBB", "Bern", "Genève Cornavin", "Winterthur", "Zürich HB"],
    )
    st.markdown(f"### Plots für {', '.join(station_list)}:")
    barplot = target.plot_ch_uplift_barplot(selectors={"Station": station_list})
    st.altair_chart(barplot, use_container_width=False)
