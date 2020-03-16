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
        frauen_18_44=Variable(
            "Frauen 18-44 Jahre", variable="md_SexAgeEk", code_nr=list(range(28, 42))
        ),
        personen_39_55_1=Variable(
            "Personen 39-55 Jahre (eng)", variable="md_agenatrep", code_nr=[2]
        ),
        personen_39_55_2=Variable(
            "Personen 39-55 Jahre (weit)", variable="md_agenatrep", code_nr=[1, 2]
        ),
        vermoegen_1mio=Variable(
            "Vermögen > CHF 1 Mio", variable="md_hhverm", code_nr=[5, 6]
        ),
        wenig_tv=Variable("Kein TV-Gerät", variable="md_tv", code_nr=[0]),
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
