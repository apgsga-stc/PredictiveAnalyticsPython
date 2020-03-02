import streamlit as st
import pandas as pd
import numpy as np
import datetime
import base64

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

st.header("File Download - A Workaround for small data")

text = """\
    There is currently (20191204) no official way of downloading data from Streamlit. See for
    example [Issue 400](https://github.com/streamlit/streamlit/issues/400)

    But I discovered a workaround
    [here](https://github.com/holoviz/panel/issues/839#issuecomment-561538340).

    It's based on the concept of
    [HTML Data URLs](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/Data_URIs)

    You can try it out below for a dataframe csv file download.

    The methodology can be extended to other file types. For inspiration see
    [base64.guru](https://base64.guru/converter/encode/file)
    """
st.markdown(text)

data = [(1, 2, 3)]
# When no file name is given, pandas returns the CSV as a string, nice.
df = pd.DataFrame(data, columns=["Col1", "Col2", "Col3"])
csv = df.to_csv(index=False)
b64 = base64.b64encode(
    csv.encode()
).decode()  # some strings <-> bytes conversions necessary here
href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
st.markdown(href, unsafe_allow_html=True)
