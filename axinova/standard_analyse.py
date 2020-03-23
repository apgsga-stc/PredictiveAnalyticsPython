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
        automotive=And(
            "Automotive",
            Or(
                "Auto nicht billig",
                Variable(name="Teures Auto", variable="g_CarHigh", code_nr=[1]),
                Variable(name="Mittelteures Auto", variable="g_CarMedium", code_nr=[1]),
            ),
            Variable(name="Autobesitz", variable="g_220", code_nr=[1, 2]),
        ),
        sportinteressiert=And(
            "Sportinteresse",
            Variable("Sportveranstaltungen", "md_872", [1]),
            Or(
                "Aktivsport",
                Variable("Einzelsport", "g_sportSingle", [1]),
                Variable("Teamsport", "g_sportMulti", [1]),
            ),
        ),
        finance=And(
            "Finance",
            Variable("Hohes Einkommen", "md_ek", [4, 5]),
            Variable("Grosses Vermögen", "md_hhverm", [4, 5, 6]),
        ),
        lifestyle=Or(
            "Lifestyle",
            Variable("Pop-Konzerte", "md_877", [1]),
            Or(
                "Lifestyle3",
                Variable("Discos, Clubs", "md_873", [1]),
                Variable("Bars, Kneipen", "md_874", [1]),
            ),
        ),
        beauty_fashion=Or(
            "Beauty, Fashion",
            Variable("Shopping", "md_875", [1]),
            Variable("Kosmetik", "md_855", [1]),
        ),
        reisen=Or(
            "Reisen",
            Variable("Vielflieger", "g_flug", [2, 3]),
            Variable("Zugfahrer", "g_privatetrainuse", [3, 4]),
        ),
        vermoegend=Variable("Vermögen > 3 Mio", "md_hhverm", [5, 6]),
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
