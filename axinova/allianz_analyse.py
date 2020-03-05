import streamlit as st

from UpliftTarget import And, Variable
from UpliftWebApp import (
    calculate_target,
    choose_stations,
    choose_target,
    describe_target,
    export_results,
    show_station_heatmaps_plot,
    show_station_weekdays,
    show_stations,
    show_summary,
    show_timeslot_plot,
    show_timeslots,
)


# define target groups
@st.cache(allow_output_mutation=True)
def create_targets() -> dict:
    targets = dict(
        age_25_44=Variable(name="Age 25-44", variable="md_agenatrep", code_nr=[1]),
        age_45_54=Variable(name="Age 45-54", variable="md_agenatrep", code_nr=[2]),
        wohnen=And(
            name="Wohnen",
            target1=Variable(name="Kind < 18", variable="md_hhu18", code_nr=[0]),
            target2=Variable(name="Haushalt > 2", variable="md_hhgr3", code_nr=[2]),
        ),
        KMU=Variable(name="KMU", variable="md_920", code_nr=[1]),
    )
    return targets


# choose target group
all_targets = create_targets()
target_key = choose_target(all_targets)

# calculate ratios for selected target
target = calculate_target(all_targets[target_key])

if st.button("Prepare XLSX file with all results"):
    export_results(target)

# show result header
station_list = choose_stations()
describe_target(target)

# choose detail result to show
results = {
    "Summary": lambda tgt, _: show_summary(tgt),
    "All Stations": lambda tgt, _: show_stations(tgt),
    "Stations / Weekdays": lambda tgt, sl: show_station_weekdays(tgt, sl),
    "Station Heatmaps": lambda tgt, sl: show_station_heatmaps_plot(tgt, sl),
    "Best Timeslots": lambda tgt, _: show_timeslots(tgt),
    "Timeslot Plots": lambda tgt, sl: show_timeslot_plot(tgt, sl),
}
result_key = st.selectbox("Choose result:", options=list(results.keys()))
results[result_key](target, station_list)
