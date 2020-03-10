import streamlit as st

from UpliftTarget import Variable, And, Or, source_data
from UpliftWebApp import (
    calculate_target,
    describe_target,
    show_timeslot_plot,
    show_summary,
    show_stations,
    show_station_weekdays,
    show_station_heatmaps_plot,
    show_timeslots,
    choose_stations,
    export_results,
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
        jung_hoch=Variable(
            "Jung, EK hoch", variable="md_SexAgeEk", code_nr=[4, 5, 32, 33]
        ),
        jung_w_mittelhoch=Variable(
            "Jung, w, EK mittel/hoch", variable="md_SexAgeEk", code_nr=[30, 31, 32, 33]
        ),
        jung_niedrig=Variable(
            "Jung, EK niedrig", variable="md_SexAgeEk", code_nr=[0, 1, 28, 29]
        ),
        jung_m_niedrig=Variable(
            "Jung, M, EK niedrig", variable="md_SexAgeEk", code_nr=[0, 1]
        ),
        jung_w_niedrig=Variable(
            "Jung, W, EK niedrig", variable="md_SexAgeEk", code_nr=[28, 29]
        ),
        jung_beauty=And(
            "Jung, Beauty/Fashion",
            Variable("Jung", variable="md_agenatrep", code_nr=[0]),
            Or(
                "Beauty, Fashion",
                Variable("Shopping", "md_875", [1]),
                Variable("Kosmetik", "md_855", [1]),
            ),
        ),
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
    "Station Heatmaps: Zielpersonen": lambda tgt, sl: show_station_heatmaps_plot(
        tgt, sl
    ),
    "Best Timeslots": lambda tgt, _: show_timeslots(tgt),
    "Timeslot Plots": lambda tgt, sl: show_timeslot_plot(tgt, sl),
}
result_key = st.selectbox("Choose result:", options=list(results.keys()))
results[result_key](target, station_list)
