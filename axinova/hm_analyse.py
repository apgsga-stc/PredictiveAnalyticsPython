import streamlit as st

from UpliftTarget import Variable, And, Or, source_data


def create_targets() -> dict:
    all_targets = dict()
    all_targets["jung_hoch"] = Variable(
        "Jung, EK hoch", variable="md_SexAgeEk", code_nr=[4, 5, 32, 33]
    )
    all_targets["jung_w_mittelhoch"] = Variable(
        "Jung, w, EK mittel/hoch", variable="md_SexAgeEk", code_nr=[30, 31, 32, 33]
    )
    all_targets["jung_niedrig"] = Variable(
        "Jung, EK niedrig", variable="md_SexAgeEk", code_nr=[0, 1, 28, 29]
    )
    all_targets["jung_m_niedrig"] = Variable(
        "Jung, M, EK niedrig", variable="md_SexAgeEk", code_nr=[0, 1]
    )
    all_targets["jung_w_niedrig"] = Variable(
        "Jung, W, EK niedrig", variable="md_SexAgeEk", code_nr=[28, 29]
    )
    all_targets["jung_beauty"] = And(
        "Jung, Beauty/Fashion",
        Variable("Jung", variable="md_agenatrep", code_nr=[0]),
        Or(
            "Beauty, Fashion",
            Variable("Shopping", "md_875", [1]),
            Variable("Kosmetik", "md_855", [1]),
        ),
    )
    return all_targets


def calculate_target(key: str) -> Variable:
    global targets
    result = targets[key]
    result.set_timescale("Hour")
    result.calculate()
    return result


targets = create_targets()

# Title
st.markdown("# Zielgruppen-Analyse")
st.sidebar.markdown("### Parameters")

# Choose Target Group
target_key = st.selectbox(
    label="Choose Target Group:",
    options=list(targets.keys()),
    index=0,
    format_func=lambda key: targets[key].name,
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
barplot = target.plot_ch_uplift_barplot(selectors={"Station": station_list})
st.altair_chart(barplot, use_container_width=False)
