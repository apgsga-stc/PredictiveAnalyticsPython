#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 4.7.2019
Visualisation for PA data
@author: kpf
"""

from IPython.core.display import display, HTML
from pa_lib.log import info


def dive(df, height=800):
    """Dive into a dataframe using Google Facets"""
    info('Converting dataframe to JSON')
    jsonstr = df.to_json(orient='records')
    html = f"""
            <script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"></script>
            <link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/master/facets-dist/facets-jupyter.html">
            <facets-dive id="elem" height="{height}"></facets-dive>
            <script>
              var data = {jsonstr};
              document.querySelector("#elem").data = data;
            </script>"""
    info('Displaying data')
    display(HTML(html))