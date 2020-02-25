import streamlit as st

from UpliftTarget import And, Variable
from UpliftWebApp import calculate_target, describe_target, show_plots, show_results


@st.cache(allow_output_mutation=True)
def create_targets() -> dict:
    all_targets = dict()
    all_targets["age_25_44"] = Variable(
        "Age 25-44", variable="md_agenatrep", code_nr=[1]
    )
    all_targets["age_45_54"] = Variable(
        "Age 45-54", variable="md_agenatrep", code_nr=[2]
    )
    all_targets["wohnen"] = And(
        "Wohnen",
        Variable("Kind < 18", variable="md_hhu18", code_nr=[0]),
        Variable("Haushalt > 2", variable="md_hhgr3", code_nr=[2]),
    )
    all_targets["KMU"] = Variable("KMU", variable="md_920", code_nr=[1])
    return all_targets


TARGETS = create_targets()

# choose target group
target_key = st.selectbox(
    label="Choose Target Group:",
    options=list(TARGETS.keys()),
    index=0,
    format_func=lambda key: TARGETS[key].name,
)

# calculate ratios
target = calculate_target(TARGETS[target_key])

# show it
describe_target(target)
show_results(target)
show_plots(target)
