import streamlit as st

from UpliftTarget import And, Or, Variable
from UpliftWebApp import (
    calculate_target,
    choose_stations,
    describe_target,
    download_results,
    show_station_heatmaps,
    download_station_heatmaps,
    show_station_weekdays,
    show_summary,
    show_timeslot_plots,
    download_timeslot_plots,
    show_timeslots,
)


# define target groups
@st.cache(allow_output_mutation=True)
def create_targets() -> dict:
    targets = dict(
        coop_pronto=And(
            "Convenience Store Public",
            Variable("Alter 20-49", variable="md_agenatrep", code_nr=[0, 1]),
            And(
                "Haushalt, Schulbildung",
                Variable("Haushaltsgrösse > 1", variable="md_hhgr3", code_nr=[1, 2]),
                Variable(
                    "Schulbildung mittel/hoch", variable="md_bildung3", code_nr=[1, 2]
                ),
            ),
        )
    )
    return targets


# choose target group
all_targets = create_targets()
target_key = st.selectbox(
    label="Choose Target Group:",
    options=list(all_targets.keys()),
    index=0,
    format_func=lambda key: all_targets[key].name,
)

# calculate ratios for selected target
target = calculate_target(all_targets[target_key])
download_results(target)

# show result header
station_list = choose_stations()
describe_target(target)

# choose detail result to show
results = {
    "Summary": lambda tgt, _: show_summary(tgt),
    "Stations / Weekdays": lambda tgt, sl: show_station_weekdays(tgt, sl),
    "Station Heatmaps: Zielkontakte": lambda tgt, sl: show_station_heatmaps(tgt, sl),
    "Download Station Heatmaps": lambda tgt, sl: download_station_heatmaps(tgt, sl),
    "Best Timeslots": lambda tgt, _: show_timeslots(tgt),
    "Timeslot Plots": lambda tgt, sl: show_timeslot_plots(tgt, sl),
    "Download Timeslot Plots": lambda tgt, sl: download_timeslot_plots(tgt, sl),
}
result_key = st.selectbox("Choose result:", options=list(results.keys()))
results[result_key](target, station_list)
