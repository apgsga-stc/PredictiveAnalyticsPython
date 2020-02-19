import streamlit as st

from UpliftTarget import Variable, And, Or, source_data
from pa_lib.data import unfactorize


def best_slots(target, column, top_n: int = 20, where=""):
    if where:
        data = target.result.query(where)
    else:
        data = target.result
    data = (
        data.sort_values(column, ascending=False)
        .head(top_n)[
            "spr target_ratio target_pers pop_uplift_ratio pop_uplift_pers".split()
        ]
        .reset_index()
        .pipe(unfactorize)
        .set_index(["Station", "DayOfWeek", "Hour"])
    )
    return unfactorize(data.reset_index())


@st.cache
def jung_hoch():
    target = Variable("Jung, EK hoch", variable="md_SexAgeEk", code_nr=[4, 5, 32, 33])
    target.set_timescale("Hour")
    target.calculate()
    return target


st.markdown("# H&M Zielgruppen-Analyse")
st.markdown("## Jung, hohes Einkommen")
TargetJungHoch = jung_hoch()

st.markdown("### Definition:")
st.write(TargetJungHoch.description())
st.markdown("### Best Slots:")
st.table(
    best_slots(TargetJungHoch, column="pop_uplift_ratio", where="target_pers >= 1000")
)
st.markdown("### Plots für Basel, Bern, Genf, Winterthur, Zürich:")
barplot = TargetJungHoch.plot_ch_uplift_barplot(
    {"Station": ["Basel SBB", "Bern", "Genève Cornavin", "Winterthur", "Zürich HB"]},
    target_col="pop_uplift_pers",
    target_threshold=0,
)
st.write(barplot)
