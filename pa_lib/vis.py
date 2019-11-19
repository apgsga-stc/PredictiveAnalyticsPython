#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 4.7.2019
Visualisation for PA data
@author: kpf
"""
import numpy as np
import seaborn as sns

from IPython.core.display import display, HTML
from matplotlib import pyplot as plt

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


########################################################################################
def boxplot_histogram(x=None, bins=None, figsize=(15, 10)):
    """Creates two plots stacked underneath each other.
       Upper plot: Boxplot. Lower plot: Histogram. Input is any array."""
    if x is None:
        x = np.random.normal(loc=1.5, scale=2, size=10000)

    sns.set(style="ticks")
    f, (ax_box, ax_hist) = plt.subplots(
        nrows=2,
        ncols=1,
        sharex="row",
        gridspec_kw={"height_ratios": (0.15, 0.85)},
        figsize=figsize,
    )

    # Boxplot
    sns.boxplot(x, notch=True, ax=ax_box)
    ax_box.set(yticks=[])
    ax_box.set_title("Boxplot")
    ax_box.grid(True)
    sns.despine(ax=ax_box, left=True)

    # Histogram
    sns.distplot(x, bins=bins, ax=ax_hist)
    ax_hist.grid(True)
    ax_hist.set_title("Histogram")
    ax_hist.set_ylabel("Percentage")
    ax_hist.set_xlabel("Value Range")
    sns.despine(ax=ax_hist)

    plt.show()