import streamlit as st
import pandas as pd
import numpy as np
import datetime

#
# Run this using "streamlit run streamlit_web_app.py" from the command line
#

# display text of several sorts
st.title("Hello World!")
st.write("This is a lot of text, let's see how it gets formatted. " * 10)
st.markdown("We can make text **bold** as well as *italic*. Or **_both!_**")
st.subheader("Some code:")
st.code(
    """import streamlit as st
import pandas as pd
import numpy as np


st.title("Hello World!")

if st.checkbox("Show dataframe"):
    chart_data = pd.DataFrame(np.random.randn(20, 3), columns=["a", "b", "c"])
    st.line_chart(chart_data)
""",
    language="python",
)

# display a DataFrame
df = pd.DataFrame(dict(nr=range(8), cls=list("aaaabbcc")))
st.subheader("A dataframe:")
st.write(df)

# interactive elements
if st.checkbox("Show graph"):
    chart_data = pd.DataFrame(np.random.randn(20, 3), columns=["a", "b", "c"])
    st.line_chart(chart_data)

# a mapbox map!
if st.checkbox("Show map"):
    df = pd.DataFrame(
        np.random.randn(1000, 2) / [50, 50] + [47.36, 8.52], columns=["lat", "lon"]
    )
    st.map(df)

# input widgets
st.subheader("An input widget:")
d = st.date_input("When's your birthday", datetime.date(2019, 7, 6))
st.write("Your birthday is:", d)

# show a dictionary
d = dict(nr=range(5), txt="abcdef", subdict={"a": 1234, "b": [1, 2, 3, 4], "c": "1234"})
st.subheader("Displaying a dictionary:")
st.write(d)