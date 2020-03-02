import streamlit as st

from UpliftTarget import And, Variable
from UpliftWebApp import calculate_target, describe_target, show_plots, show_results


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
target_key = st.selectbox(
    label="Choose Target Group:",
    options=list(all_targets.keys()),
    index=0,
    format_func=lambda key: all_targets[key].name,
)

# calculate ratios for selected target
target = calculate_target(all_targets[target_key])

# show results
describe_target(target)
show_results(target)
show_plots(target)

if st.button("Store results as XLSX"):
    target.export_result()
