import streamlit as st

from UpliftTarget import Variable, And, Or, source_data


def create_targets() -> dict:
    all_targets = dict()
    all_targets["automotive"] = And(
        "Automotive",
        Or(
            "Auto nicht billig",
            Variable(name="Teures Auto", variable="g_CarHigh", code_nr=[1]),
            Variable(name="Mittelteures Auto", variable="g_CarMedium", code_nr=[1]),
        ),
        Variable(name="Autobesitz", variable="g_220", code_nr=[1, 2]),
    )
    all_targets["sportinteressiert"] = And(
        "Sportinteresse",
        Variable("Sportveranstaltungen", "md_872", [1]),
        Or(
            "Aktivsport",
            Variable("Einzelsport", "g_sportSingle", [1]),
            Variable("Teamsport", "g_sportMulti", [1]),
        ),
    )
    all_targets["finance"] = And(
        "Finance",
        Variable("Hohes Einkommen", "md_ek", [4, 5]),
        Variable("Grosses Vermögen", "md_hhverm", [4, 5, 6]),
    )
    all_targets["lifestyle"] = Or(
        "Lifestyle",
        Variable("Pop-Konzerte", "md_877", [1]),
        Or(
            "Lifestyle3",
            Variable("Discos, Clubs", "md_873", [1]),
            Variable("Bars, Kneipen", "md_874", [1]),
        ),
    )
    all_targets["beauty_fashion"] = Or(
        "Beauty, Fashion",
        Variable("Shopping", "md_875", [1]),
        Variable("Kosmetik", "md_855", [1]),
    )
    all_targets["reisen"] = Or(
        "Reisen",
        Variable("Vielflieger", "g_flug", [2, 3]),
        Variable("Zugfahrer", "g_privatetrainuse", [3, 4]),
    )
    return all_targets


def calculate_target(key: str) -> Variable:
    global TARGETS
    result = targets[key]
    result.set_timescale("Hour")
    result.calculate()
    return result


TARGETS = create_targets()

# Title
st.markdown("# Zielgruppen-Analyse")
st.sidebar.markdown("### Parameters")

# Choose Target Group
target_key = st.selectbox(
    label="Choose Target Group:",
    options=list(TARGETS.keys()),
    index=0,
    format_func=lambda key: TARGETS[key].name,
)
target = calculate_target(target_key)
st.markdown(f"## Target Group: {target.name}")

st.markdown("### Definition:")
st.text(target.description())

# Result Tables
st.markdown("### Summary:")
st.table(target.result_summary())

st.markdown("### Stations per week:")
st.table(target.best_stations(where="spr > 0"))

nr_best_slots = st.sidebar.number_input(
    "Number of Best Slots:", min_value=1, max_value=500, value=20, format="%d"
)
target_min = st.sidebar.number_input(
    "Minimum Target Persons:", min_value=0, max_value=1000, value=360, format="%d"
)
st.markdown(f"### Best Slots by Ratio (Target Persons >= {target_min}):")
st.table(
    target.best_slots(
        column=["pop_uplift_ratio", "target_pers"],
        where=f"target_pers >= {target_min}",
        top_n=nr_best_slots,
    )
)

# Result Plot
station_list = st.sidebar.multiselect(
    label="Interesting Stations:",
    options=source_data.all_stations,
    default=["Basel SBB", "Bern", "Genève Cornavin", "Winterthur", "Zürich HB"],
)
st.markdown(f"### Plots für {', '.join(station_list)}:")
barplot = target.plot_timeslot_plots(selectors={"Station": station_list})
st.altair_chart(barplot, use_container_width=False)
