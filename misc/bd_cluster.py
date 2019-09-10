#!/usr/bin/env python
# coding: utf-8

# # Libraries & Settings
# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
import numpy as np
import qgrid
from datetime import datetime as dtt

from pa_lib.file import (
    data_files,
    load_bin,
    store_bin,
    load_csv,
    write_xlsx,
    load_xlsx,
    set_project_dir,
    project_dir,
)
from pa_lib.data import (
    calc_col_partitioned,
    clean_up_categoricals,
    unfactorize,
    flatten,
    replace_col,
    cond_col,
    desc_col,
    unfactorize,
    as_dtype,
    flatten_multi_index_cols,
)
from pa_lib.util import obj_size, cap_words, normalize_rows, clear_row_max
from pa_lib.log import time_log, info
from pa_lib.vis import dive

# display long columns completely, show more rows
pd.set_option("display.max_colwidth", 200)
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 200)


def qshow(df, fit_width=False):
    return qgrid.show_grid(
        df, grid_options={"forceFitColumns": fit_width, "fullWidthRows": False}
    )


# # Load data

# In[74]:


set_project_dir("vkprog")

bd_raw = load_bin("bd_data.feather").rename(
    mapper=lambda name: cap_words(name, sep="_"), axis="columns"
)
bd = bd_raw.loc[(bd_raw.Netto > 0)].pipe(clean_up_categoricals)


# In[4]:


desc_col(bd)


# # Prepare Endkunden Information

# In[ ]:


def last_notna(s):
    try:
        return s.loc[s.last_valid_index()]
    except KeyError:
        return np.NaN


def collect(s, sep=","):
    return sep.join(map(str, s[s.notna()].unique()))


# this takes around 85 seconds
with time_log("preparing EK_INFO"):
    ek_info = (
        bd.sort_values(["Endkunde_NR", "Kampagne_Erfassungsdatum"])
        .astype({"Endkunde_NR": "int64", "Kamp_Erfass_Jahr": "int16"})
        .groupby("Endkunde_NR")
        .agg(
            {
                "Endkunde": last_notna,
                "EK_Aktiv": last_notna,
                "EK_Land": last_notna,
                "EK_Plz": last_notna,
                "EK_Ort": last_notna,
                "Agentur": last_notna,
                "Endkunde_Branchengruppe": last_notna,
                "Endkunde_Branchengruppe_ID": last_notna,
                "Auftrag_Branchengruppe_ID": [collect, "nunique"],
                "Kampagne_Erfassungsdatum": "max",
                "Kamp_Erfass_Jahr": ["min", "max"],
                "Kampagne_Beginn": "max",
            }
        )
    )

ek_info.set_axis(
    labels="""Endkunde EK_Aktiv EK_Land EK_Plz EK_Ort Agentur EK_BG EK_BG_ID Auftrag_BG_ID Auftrag_BG_Anz
              Last_Res_Date First_Res_Year Last_Res_Year Last_Aus_Date""".split(),
    axis="columns",
    inplace=True,
)


# In[6]:


desc_col(ek_info)


# In[7]:


ek_info.head(10)


# ### How many customers started or ended in which year?

# In[8]:


pd.crosstab(index=ek_info.First_Res_Year, columns=ek_info.Last_Res_Year, margins=True)


# In[9]:


import seaborn as sns
import matplotlib.pyplot as plt

plot_data = pd.crosstab(index=ek_info.First_Res_Year, columns=ek_info.Last_Res_Year)

plt.figure(figsize=(15, 10))
sns.heatmap(data=plot_data, linewidths=0.3, vmax=1500, annot=True, fmt="d")


# ### Store and reload result

# In[ ]:


store_bin(ek_info, "bd_cluster_ek_info.feather")


# In[5]:


ek_info = load_bin("bd_cluster_ek_info.feather")


# # Distribution of Auftragsart

# In[10]:


display(
    pd.crosstab(
        index=[bd.Auftragsart, bd.Vertrag], columns=bd.Kamp_Erfass_Jahr, margins=True
    )
)
display("Netto-Umsatz")
display(
    bd.pivot_table(
        index=["Auftragsart", "Vertrag"],
        columns="Kamp_Erfass_Jahr",
        values="Netto",
        aggfunc="sum",
        fill_value=0,
        margins=True,
    )
)


# ### Auftragsart vs. customers

# In[11]:


get_ipython().run_cell_magic(
    "time",
    "",
    "\nek_auftragsart = pd.crosstab(columns=df_bookings.Auftragsart, index=df_bookings.Endkunde_NR)\nek_auftragsart.mask(ek_auftragsart == 0, inplace=True)\n\nek_auftragsart.head(10)",
)


# How many **different** Auftragsart per customer?

# In[12]:


ek_auftragsart.count(axis="columns").value_counts()


# How many **most frequent** Auftragsart per customer?

# In[13]:


(ek_auftragsart.subtract(ek_auftragsart.max(axis="columns"), axis="index") == 0).sum(
    axis="columns"
).value_counts()


# Where is the a **clear favourite** Auftragsart for a customer?

# In[14]:


get_ipython().run_cell_magic(
    "time", "", "ek_auftragsart_scores = clear_row_max(ek_auftragsart)"
)


# In[15]:


ek_auftragsart_scoring = pd.DataFrame(
    index=ek_auftragsart_scores.index, data={"AufArt": ek_auftragsart_scores}
)

ek_auftragsart_scoring.dropna(inplace=True)


# # Branchen

# In[16]:


ek_branchen = ek_info.loc[:, ["Endkunde_NR", "EK_BG_ID", "Auftrag_BG_ID"]]


# In[17]:


def unique_list(lst):
    return list(set(lst))


ek_branchen["Auftrag_BG_list"] = ek_branchen.Auftrag_BG_ID.str.split(",")
ek_branchen["BG_list"] = ek_branchen.apply(
    lambda x: unique_list([x.EK_BG_ID] + x.Auftrag_BG_list), axis="columns"
)
ek_branchen.drop(
    ["EK_BG_ID", "Auftrag_BG_ID", "Auftrag_BG_list"], axis="columns", inplace=True
)


# In[18]:


display(ek_branchen.head())

ek_branchen_scoring = ek_branchen.explode(column="BG_list").rename(
    columns={"BG_list": "BG"}
)

display(ek_branchen_scoring.head())


# # Auftragsart vs. Branchen

# In[19]:


AufArt_BG = ek_auftragsart_scoring.merge(
    ek_branchen_scoring, how="inner", on="Endkunde_NR"
)


# In[20]:


plt.figure(figsize=(20, 32))
sns.heatmap(
    data=np.log1p(
        pd.crosstab(columns=AufArt_BG.AufArt, index=AufArt_BG.BG, margins=True)
        .sort_values("All", ascending=False)
        .sort_values("All", axis="columns", ascending=False)
    ).iloc[1:, 1:]
)


# In[21]:


display(
    normalize_rows(
        pd.crosstab(columns=AufArt_BG.AufArt, index=AufArt_BG.BG, margins=True)
        .sort_values("All", ascending=False)
        .sort_values("All", axis="columns", ascending=False)
        .iloc[1:, 1:]
    )
)


# # Restrict bookings to interesting customers (current & long-term)

# ### List of current long-time customers
#
# * "Current" means that their last booking was not more than two years back.
# * "Long-time" means that they had at least two years of bookings.

# In[22]:


limit_year = pd.Timestamp.today().year - 2

ek_nr_current = ek_info.loc[
    (ek_info.Last_Res_Year >= limit_year) & (ek_info.First_Res_Year < limit_year - 2),
    "Endkunde_NR",
]


# In[23]:


bd_current = bd.loc[bd.Endkunde_NR.isin(ek_nr_current)].pipe(clean_up_categoricals)

print(f"Keeping {bd_current.shape[0]} of {bd.shape[0]} records")


# In[24]:


ek_info_current = ek_info.loc[ek_info.Endkunde_NR.isin(ek_nr_current)]

print(f"Keeping {ek_info_current.shape[0]} of {ek_info.shape[0]} records")


# # Aggregate bookings per customer, year, and KW_2 / KW_4 period
#
# Both by Reservation and Aushang.

# In[25]:


def sum_calc(df, col_year, col_week):
    return (
        df.loc[:, ["Endkunde_NR", col_year, col_week, "Netto"]]
        .pipe(unfactorize)
        .groupby(["Endkunde_NR", col_year, col_week], observed=True, as_index=False)
        .agg({"Netto": ["sum"]})
        .set_axis(
            f"Endkunde_NR {col_year} {col_week} Netto_Sum".split(),
            axis="columns",
            inplace=False,
        )
    )


def aggregate_bookings(df, period):
    info(f"Period: {period}")
    info("Calculate Reservation...")
    df_res = sum_calc(df, "Kamp_Erfass_Jahr", f"Kamp_Erfass_{period}")
    info("Calculate Aushang...")
    df_aus = sum_calc(df, "Kamp_Beginn_Jahr", f"Kamp_Beginn_{period}")

    info("Merge Results...")
    df_aggr = df_res.merge(
        right=df_aus,
        left_on=["Endkunde_NR", "Kamp_Erfass_Jahr", f"Kamp_Erfass_{period}"],
        right_on=["Endkunde_NR", "Kamp_Beginn_Jahr", f"Kamp_Beginn_{period}"],
        how="outer",
        suffixes=("_Res", "_Aus"),
    ).rename(
        {"Kamp_Erfass_Jahr": "Jahr", f"Kamp_Erfass_{period}": period}, axis="columns"
    )

    df_aggr = (
        df_aggr.fillna(
            {
                "Jahr": df_aggr.Kamp_Beginn_Jahr,
                period: df_aggr[f"Kamp_Beginn_{period}"],
                "Netto_Sum_Res": 0,
                "Netto_Sum_Aus": 0,
            }
        )
        .drop(["Kamp_Beginn_Jahr", f"Kamp_Beginn_{period}"], axis="columns")
        .astype({"Jahr": "int16"})
        .astype({period: "int8"})
        .sort_values(["Jahr", "Endkunde_NR", period])
        .reset_index(drop=True)
    )

    return df_aggr


# In[26]:


bd_aggr_2w = aggregate_bookings(bd_current, "KW_2")
bd_aggr_4w = aggregate_bookings(bd_current, "KW_4")


# In[27]:


bd_aggr_2w.head(10)


# # Netto by customer / year

# In[28]:


bd_aggr_yr = bd_aggr_2w.groupby(["Endkunde_NR", "Jahr"]).agg(
    {"Netto_Sum_Res": "sum", "Netto_Sum_Aus": "sum"}
)

bd_aggr_yr.head(15)


# In[30]:


bd_aggr_yr_ek = bd_aggr_yr.groupby("Endkunde_NR").agg(["min", "max", "mean", "median"])

bd_aggr_yr_ek.head()


# In[31]:


np.log1p(bd_aggr_yr_ek[("Netto_Sum_Res", "mean")]).hist()


# ### Find good category borders for 5 categories

# In[32]:


pd.qcut(bd_aggr_yr_ek[("Netto_Sum_Res", "mean")].values, q=5).categories


# ### Cut the net values, label categories, merge back into ``ek_info_current``

# In[33]:


_netto_cat = pd.cut(
    bd_aggr_yr_ek[("Netto_Sum_Res", "mean")],
    bins=(0, 4000, 10000, 50000, 500000, 7000000),
    labels="<4k 4k-10k 10k-50k 50k-500k >500k".split(),
).rename("netto_cat")

ek_info_current.drop(columns="netto_cat", inplace=True, errors="ignore")
ek_info_current = ek_info_current.merge(_netto_cat, on="Endkunde_NR")

ek_info_current.netto_cat.value_counts()


# # Calculate sum curve per customer * year, over periods

# In[34]:


def make_year_grp_sumcurve(df, year_col, grp_col, data_col, prefix=""):
    # build new columns with sum/cumsum per year/grp
    df = df.pipe(
        calc_col_partitioned,
        f"{prefix}sumJahr",
        fun="sum",
        on=data_col,
        part_by=[year_col, grp_col],
    ).pipe(
        calc_col_partitioned,
        f"{prefix}cumJahr",
        fun="cumsum",
        on=data_col,
        part_by=[year_col, grp_col],
    )

    # divide data_col by sum to get scaled amounts, show in %
    df = (
        df.eval(f"{prefix}prcJahr = ({data_col} / {prefix}sumJahr) * 100 + 0.5")
        .fillna({f"{prefix}prcJahr": 0})
        .astype({f"{prefix}prcJahr": "int"})
    )

    # divide cumsum by sum to get sum curve [0, 1], show in %
    df = (
        df.eval(f"{prefix}crvJahr = ({prefix}cumJahr / {prefix}sumJahr) * 100 + 0.5")
        .fillna({f"{prefix}crvJahr": 0})
        .astype({f"{prefix}crvJahr": "int"})
    )
    return df


# In[35]:


bd_aggr_2w = (
    bd_aggr_2w.pipe(
        make_year_grp_sumcurve,
        year_col="Jahr",
        grp_col="Endkunde_NR",
        data_col="Netto_Sum_Res",
        prefix="Res_",
    )
    .pipe(
        make_year_grp_sumcurve,
        year_col="Jahr",
        grp_col="Endkunde_NR",
        data_col="Netto_Sum_Aus",
        prefix="Aus_",
    )
    .sort_values(["Endkunde_NR", "Jahr", "KW_2"])
    .reset_index(drop=True)
)

bd_aggr_4w = (
    bd_aggr_4w.pipe(
        make_year_grp_sumcurve,
        year_col="Jahr",
        grp_col="Endkunde_NR",
        data_col="Netto_Sum_Res",
        prefix="Res_",
    )
    .pipe(
        make_year_grp_sumcurve,
        year_col="Jahr",
        grp_col="Endkunde_NR",
        data_col="Netto_Sum_Aus",
        prefix="Aus_",
    )
    .sort_values(["Endkunde_NR", "Jahr", "KW_4"])
    .reset_index(drop=True)
)


# In[36]:


bd_aggr_2w.head(10)


# In[37]:


bd_aggr_4w.head(10)


# ### Store and reload results

# In[38]:


store_bin(bd_aggr_2w, "bd_cluster_aggr_2w.feather")
store_bin(bd_aggr_4w, "bd_cluster_aggr_4w.feather")


# In[39]:


bd_aggr_2w = load_bin("bd_cluster_aggr_2w.feather")
bd_aggr_4w = load_bin("bd_cluster_aggr_4w.feather")


# # Clustering Customers by Reservation

# ### Drop incomplete years
#
# We have data for the last 10 years, the current year is incomplete

# In[40]:


valid_years = list(range(pd.Timestamp.today().year - 10, pd.Timestamp.today().year))
valid_years


# ### Pivot prc values by period

# In[41]:


bd_res_prc_2w_data = bd_aggr_2w.loc[(bd_aggr_2w.Jahr.isin(valid_years))].pivot_table(
    index=["Endkunde_NR", "Jahr"],
    columns="KW_2",
    values="Res_prcJahr",
    aggfunc="sum",
    fill_value=0,
)

# drop years with no reservations
bd_res_prc_2w_data = bd_res_prc_2w_data.loc[bd_res_prc_2w_data.sum(axis="columns") > 0]

bd_res_prc_2w_data.head(12)


# ### Prepare data: Aggregate over years by customer

# In[42]:


ek_2w_prc_mean = (
    bd_res_prc_2w_data.reset_index()
    .drop("Jahr", axis="columns")
    .groupby("Endkunde_NR")
    .agg("mean")
)

# correct rowsums to 100
ek_2w_prc_mean = normalize_rows(ek_2w_prc_mean) * 100

ek_2w_prc_mean_stack = (
    ek_2w_prc_mean.stack()
    .reset_index()
    .set_axis(["Endkunde_NR", "KW_2", "prc_mean"], axis="columns", inplace=False)
)

display(ek_2w_prc_mean.round(1).head(10))


# ### Pivot crv values by period

# In[43]:


bd_res_crv_2w_data = (
    bd_aggr_2w.loc[(bd_aggr_2w.Jahr.isin(valid_years))]
    .pivot_table(
        index=["Endkunde_NR", "Jahr"],
        columns="KW_2",
        values="Res_crvJahr",
        aggfunc="sum",
    )
    .fillna(method="ffill", axis=1)
    .fillna(0)
)

# drop years with no reservations
bd_res_crv_2w_data = bd_res_crv_2w_data.loc[bd_res_crv_2w_data.sum(axis="columns") > 0]


# ### Aggregate over years by customer

# In[44]:


ek_2w_crv_mean = (
    bd_res_crv_2w_data.reset_index()
    .drop("Jahr", axis="columns")
    .groupby("Endkunde_NR")
    .agg("mean")
)

ek_2w_crv_mean_stack = (
    ek_2w_crv_mean.stack()
    .reset_index()
    .set_axis(["Endkunde_NR", "KW_2", "crv_mean"], axis="columns", inplace=False)
)

display(bd_res_crv_2w_data.head())
display(ek_2w_crv_mean.head())
ek_2w_crv_mean_stack.head()


# ### Plot customer booking patterns (global)

# In[46]:


import seaborn as sns

get_ipython().run_line_magic("matplotlib", "inline")

sns.set()
sns.catplot(data=ek_2w_prc_mean_stack, x="KW_2", y="prc_mean", aspect=2.5, kind="boxen")


# In[47]:


sns.catplot(data=ek_2w_crv_mean_stack, x="KW_2", y="crv_mean", aspect=2.5, kind="boxen")


# ### Cluster booking patterns (percentage of yearly sum)
#
# Prepare data

# In[54]:


X = ek_2w_prc_mean.to_numpy()

X_columns = ek_2w_prc_mean.columns


# #### Normal KMeans

# In[55]:


from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score


# In[ ]:


for n_clusters in range(5, 21):
    kmeans_ = KMeans(n_clusters=n_clusters, random_state=0, n_jobs=-1)
    cluster_labels_ = kmeans_.fit_predict(X)
    ch_score = calinski_harabasz_score(X, cluster_labels_)
    sil_score = silhouette_score(X, cluster_labels_)
    print(
        f"For n_clusters = {n_clusters}: Silhouette_score = {sil_score},  CH score = {ch_score}"
    )


# In[56]:


nkmeans = KMeans(n_clusters=15, random_state=0, verbose=1, n_jobs=-1)

prc_nkmeans_labels = nkmeans.fit_predict(X)


# Show cluster centroids sorted by cluster size (number of rows)

# In[57]:


import seaborn as sns
import matplotlib.pyplot as plt

nkmeans_clusters = pd.DataFrame(data=nkmeans.cluster_centers_, columns=X_columns).loc[
    pd.Series(prc_nkmeans_labels).value_counts().index
]

plt.figure(figsize=(20, 10))
sns.heatmap(nkmeans_clusters, annot=True, vmin=1, vmax=5)


# Cluster sizes

# In[58]:


pd.Series(prc_nkmeans_labels).value_counts()


# Plot scaled bookings by cluster

# In[59]:


plot_data = ek_2w_prc_mean_stack.merge(
    pd.Series(data=prc_nkmeans_labels, index=ek_2w_prc_mean.index, name="cluster"),
    on="Endkunde_NR",
)

sns.catplot(
    data=plot_data,
    x="KW_2",
    y="prc_mean",
    aspect=1.1,
    kind="boxen",
    col="cluster",
    col_wrap=4,
    col_order=pd.Series(prc_nkmeans_labels)
    .value_counts()
    .index,  # order by cluster size
    height=4,
)


# ### Cluster booking patterns (cumulative percentage of yearly sum)
#
# Prepare data

# In[60]:


X = ek_2w_crv_mean.to_numpy()

X_columns = ek_2w_crv_mean.columns


# #### Normal KMeans

# In[61]:


from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score


# In[ ]:


for n_clusters in range(3, 21):
    kmeans_ = KMeans(n_clusters=n_clusters, random_state=0, n_jobs=-1)
    cluster_labels_ = kmeans_.fit_predict(X)
    ch_score = calinski_harabasz_score(X, cluster_labels_)
    sil_score = silhouette_score(X, cluster_labels_)
    print(
        f"For n_clusters = {n_clusters}: Silhouette_score = {sil_score},  CH score = {ch_score}"
    )


# In[62]:


nkmeans = KMeans(n_clusters=15, random_state=0, verbose=1, n_jobs=-1)

crv_nkmeans_labels = nkmeans.fit_predict(X)


# Show cluster centroids sorted by cluster size (number of rows)

# In[63]:


import seaborn as sns
import matplotlib.pyplot as plt

nkmeans_clusters = pd.DataFrame(data=nkmeans.cluster_centers_, columns=X_columns).loc[
    pd.Series(crv_nkmeans_labels).value_counts().index
]

plt.figure(figsize=(20, 10))
sns.heatmap(nkmeans_clusters, annot=True)


# Cluster sizes

# In[64]:


pd.Series(crv_nkmeans_labels).value_counts()


# Plot scaled bookings by cluster

# In[65]:


plot_data = ek_2w_prc_mean_stack.merge(
    pd.Series(data=crv_nkmeans_labels, index=ek_2w_crv_mean.index, name="cluster"),
    on="Endkunde_NR",
)

sns.catplot(
    data=plot_data,
    x="KW_2",
    y="prc_mean",
    aspect=1.5,
    kind="boxen",
    col="cluster",
    col_wrap=3,
    col_order=pd.Series(crv_nkmeans_labels)
    .value_counts()
    .index,  # order by cluster size
    height=4,
)


# # PCA on Customers

# ### Prepare Data

# In[ ]:


X = ek_2w_crv_mean.to_numpy()
X_columns = ek_2w_crv_mean.columns
X_index = ek_2w_crv_mean.index

y = crv_nkmeans_labels


# ### Calculate standard PCA with 4 components

# In[ ]:


from sklearn.decomposition import PCA

pca = PCA(n_components=4, random_state=0)
X_pca = pca.fit_transform(X)


# In[ ]:


pca.explained_variance_ratio_


# In[ ]:


pca.components_


# ### Plot K-means clusters against principal components

# In[ ]:


plot_data = pd.DataFrame(X_pca, index=X_index, columns=range(1, pca.n_components + 1))
plot_data["clusters"] = y
sns.set(style="ticks")
sns.pairplot(data=plot_data, hue="clusters", plot_kws={"alpha": 0.4})


# # Clustering Customer Booking Years

# ### Prepare data: Pivot prc values by period

# In[ ]:


bd_res_prc_2w_data = bd_aggr_2w.loc[(bd_aggr_2w.Jahr.isin(valid_years))].pivot_table(
    index=["Endkunde_NR", "Jahr"],
    columns="KW_2",
    values="Res_prcJahr",
    aggfunc="sum",
    fill_value=0,
)

# drop years with no reservations
bd_res_prc_2w_data = bd_res_prc_2w_data.loc[bd_res_prc_2w_data.sum(axis="columns") > 0]

bd_res_prc_2w_data_stack = bd_res_prc_2w_data.stack()

bd_res_prc_2w_data.head(12)


# In[ ]:


X = bd_res_prc_2w_data.to_numpy()

X_columns = bd_res_prc_2w_data.columns
X_index = bd_res_prc_2w_data.index


# #### Normal KMeans

# In[ ]:


from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score


# In[ ]:


for n_clusters in range(25, 31):
    kmeans_ = KMeans(n_clusters=n_clusters, random_state=0, n_jobs=-1)
    cluster_labels_ = kmeans_.fit_predict(X)
    ch_score = calinski_harabasz_score(X, cluster_labels_)
    sil_score = silhouette_score(X, cluster_labels_)
    print(
        f"For n_clusters = {n_clusters}: Silhouette_score = {sil_score},  CH score = {ch_score}"
    )


# In[ ]:


nkmeans = KMeans(n_clusters=27, random_state=0, verbose=1, n_jobs=-1)

yr_prc_nkmeans_labels = nkmeans.fit_predict(X)


# In[ ]:


import seaborn as sns
import matplotlib.pyplot as plt

yr_prc_nkmeans_clusters = pd.DataFrame(
    data=nkmeans.cluster_centers_, columns=X_columns
).loc[pd.Series(yr_prc_nkmeans_labels).value_counts().index]

plt.figure(figsize=(25, 16))
sns.heatmap(yr_prc_nkmeans_clusters, annot=True, vmin=0, vmax=7)


# In[ ]:


pd.Series(yr_prc_nkmeans_labels).value_counts()  # / yr_prc_nkmeans_labels.shape[0]


# In[ ]:


plot_data = (
    pd.DataFrame.from_dict({"prc": bd_res_prc_2w_data_stack})
    .reset_index()
    .merge(
        pd.Series(
            data=yr_prc_nkmeans_labels, index=bd_res_prc_2w_data.index, name="cluster"
        ),
        on=["Endkunde_NR", "Jahr"],
    )
)

sns.catplot(
    data=plot_data,
    x="KW_2",
    y="prc",
    aspect=1.5,
    kind="boxen",
    col="cluster",
    col_wrap=3,
    height=4,
)


# ### Clusters matched to customers

# In[ ]:


ek_prc_cls = pd.crosstab(
    index=bd_res_prc_2w_data.reset_index()["Endkunde_NR"],
    columns=pd.Series(yr_prc_nkmeans_labels, name="cluster"),
)

ek_prc_cls_yr_cnt_ = ek_prc_cls.copy()


def cls_list(s):
    return list(s.loc[s > 0].index.values)


def main_cls_list(s):
    return list(s.loc[s == np.max(s)].index.values)


ek_prc_cls = ek_prc_cls.assign(
    n_yr=ek_prc_cls_yr_cnt_.sum(axis="columns"),
    n_cls=ek_prc_cls_yr_cnt_.where(ek_prc_cls > 0).count(axis="columns"),
    cls=ek_prc_cls_yr_cnt_.apply(cls_list, axis="columns"),
    main_cls=ek_prc_cls_yr_cnt_.apply(main_cls_list, axis="columns"),
)
ek_prc_cls["n_main_cls"] = ek_prc_cls["main_cls"].apply(len)

ek_prc_cls


# ### Customers that can be related to 1 or 2 clusters

# In[ ]:


max_main_cls = 2

ek_clust_scoring = ek_prc_cls.query(
    "n_main_cls < n_cls and n_main_cls <= @max_main_cls"
)[["main_cls"]]


# ### Prepare data: Pivot crv values by period

# In[ ]:


bd_res_crv_2w_data = (
    bd_aggr_2w.loc[(bd_aggr_2w.Jahr.isin(valid_years))]
    .pivot_table(
        index=["Endkunde_NR", "Jahr"],
        columns="KW_2",
        values="Res_crvJahr",
        aggfunc="sum",
    )
    .fillna(method="ffill", axis=1)
    .fillna(0)
)

# drop years with no reservations
bd_res_crv_2w_data = bd_res_crv_2w_data.loc[bd_res_crv_2w_data.sum(axis="columns") > 0]

bd_res_crv_2w_data_stack = bd_res_crv_2w_data.stack()

bd_res_crv_2w_data.head(12)


# ### Prepare for clustering

# In[ ]:


X = bd_res_crv_2w_data.to_numpy()

X_columns = bd_res_crv_2w_data.columns
X_index = bd_res_crv_2w_data.index


# #### Normal KMeans

# In[ ]:


from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score


# In[ ]:


for n_clusters in [15, 25, 35, 40]:
    kmeans_ = KMeans(n_clusters=n_clusters, random_state=0, n_jobs=-1)
    cluster_labels_ = kmeans_.fit_predict(X)
    ch_score = calinski_harabasz_score(X, cluster_labels_)
    sil_score = silhouette_score(X, cluster_labels_)
    print(
        f"For n_clusters = {n_clusters}: Silhouette_score = {sil_score},  CH score = {ch_score}"
    )


# ```
# For n_clusters = 5: Silhouette_score = 0.3049945347740261,  CH score = 29630.89896114061
# For n_clusters = 10: Silhouette_score = 0.366230862992324,  CH score = 26973.062027368196
# For n_clusters = 15: Silhouette_score = 0.40334656127747526,  CH score = 25417.006073311764
# For n_clusters = 20: Silhouette_score = 0.44202548618962784,  CH score = 24818.708849953175
# For n_clusters = 25: Silhouette_score = 0.5033501558327148,  CH score = 24748.31007457299
# For n_clusters = 30: Silhouette_score = 0.5602936199726821,  CH score = 25025.39923837368
# For n_clusters = 35: Silhouette_score = 0.6095264482491675,  CH score = 26642.345879735167
# For n_clusters = 40: Silhouette_score = 0.6171105582735361,  CH score = 26586.30470955651
# ```

# In[ ]:


nkmeans = KMeans(n_clusters=27, random_state=0, verbose=1, n_jobs=-1)

yr_crv_nkmeans_labels = nkmeans.fit_predict(X)


# ### Show cluster centroids

# In[ ]:


import seaborn as sns
import matplotlib.pyplot as plt

yr_nkmeans_clusters = pd.DataFrame(data=nkmeans.cluster_centers_, columns=X_columns)

plt.figure(figsize=(25, 16))
sns.heatmap(yr_nkmeans_clusters, annot=True, vmin=5)


# ### Cluster sizes

# In[ ]:


pd.Series(yr_crv_nkmeans_labels).value_counts()


# In[ ]:


plot_data = (
    pd.DataFrame.from_dict({"prc": bd_res_prc_2w_data_stack})
    .reset_index()
    .merge(
        pd.Series(
            data=yr_crv_nkmeans_labels, index=bd_res_crv_2w_data.index, name="cluster"
        ),
        on=["Endkunde_NR", "Jahr"],
    )
)

sns.catplot(
    data=plot_data,
    x="KW_2",
    y="prc",
    aspect=1.5,
    kind="boxen",
    col="cluster",
    col_wrap=3,
    height=4,
)


# ### Clusters matched to customers

# In[ ]:


ek_cls = pd.crosstab(
    index=bd_res_crv_2w_data.reset_index()["Endkunde_NR"],
    columns=pd.Series(yr_crv_nkmeans_labels, name="cluster"),
)

ek_cls_yr_cnt_ = ek_cls.copy()


def cls_list(s):
    return list(s.loc[s > 0].index.values)


def main_cls_list(s):
    return list(s.loc[s == np.max(s)].index.values)


ek_cls = ek_cls.assign(
    n_yr=ek_cls_yr_cnt_.sum(axis="columns"),
    n_cls=ek_cls_yr_cnt_.where(ek_cls > 0).count(axis="columns"),
    cls=ek_cls_yr_cnt_.apply(cls_list, axis="columns"),
    main_cls=ek_cls_yr_cnt_.apply(main_cls_list, axis="columns"),
)
ek_cls["n_main_cls"] = ek_cls["main_cls"].apply(len)

ek_cls


# ### Customers that can be assigned to 1 or 2 clusters

# In[ ]:


max_main_cls = 2

yr_clust_scoring = ek_cls.query("n_main_cls < n_cls and n_main_cls <= @max_main_cls")[
    ["main_cls"]
]


# # Compare scoring between customer layer and year layer

# In[ ]:


len(np.unique(ek_clust_scoring.index.values))


# In[ ]:


len(np.unique(yr_clust_scoring.index.values))


# In[ ]:


pd.DataFrame({"ek": np.unique(ek_clust_scoring.index.values)}).merge(
    pd.DataFrame({"ek": np.unique(yr_clust_scoring.index.values)}),
    how="outer",
    on="ek",
    indicator=True,
)["_merge"].value_counts()


# # Cluster Scoring vs. Auftragsart

# In[ ]:


AufArt_Clust = ek_auftragsart_scoring.merge(
    yr_clust_scoring.explode(column="main_cls"), how="inner", on="Endkunde_NR"
).rename(columns={"main_cls": "Clust"})


# In[ ]:


pd.crosstab(columns=AufArt_Clust.AufArt, index=AufArt_Clust.Clust)


# In[ ]:


plt.figure(figsize=(16, 20))
sns.heatmap(
    data=np.log1p(pd.crosstab(columns=AufArt_Clust.AufArt, index=AufArt_Clust.Clust)),
    annot=pd.crosstab(columns=AufArt_Clust.AufArt, index=AufArt_Clust.Clust),
    fmt="d",
)


# # Cluster Scoring vs. Branchengruppe

# In[ ]:


BG_Clust = ek_branchen_scoring.merge(
    yr_clust_scoring.explode(column="main_cls"), how="inner", on="Endkunde_NR"
).rename(columns={"main_cls": "Clust"})


# In[ ]:


pd.crosstab(columns=BG_Clust.Clust, index=BG_Clust.BG, margins=True)


# In[ ]:


plt.figure(figsize=(16, 32))
sns.heatmap(
    data=normalize_rows(pd.crosstab(columns=BG_Clust.Clust, index=BG_Clust.BG)),
    annot=pd.crosstab(columns=BG_Clust.Clust, index=BG_Clust.BG),
    fmt="d",
    vmax=0.4,
)


# # Cluster Scoring vs. Kanton

# In[76]:


with project_dir("Raumgliederung"):
    ek_region = load_bin("ek_region.feather")


# In[78]:


ek_region.head()


# In[79]:


Kt_Clust = (
    ek_region[["Endkunde_NR", "KANTON"]]
    .merge(yr_clust_scoring.explode(column="main_cls"), how="inner", on="Endkunde_NR")
    .rename(columns={"main_cls": "Clust"})
)


# In[ ]:


plt.figure(figsize=(16, 16))
sns.heatmap(
    data=normalize_rows(pd.crosstab(columns=Kt_Clust.Clust, index=Kt_Clust.KANTON)),
    annot=pd.crosstab(columns=Kt_Clust.Clust, index=Kt_Clust.KANTON),
    fmt="d",
    # vmax=0.4
)


# # Cluster Scoring vs. Grossregion

# In[ ]:


GrReg_Clust = (
    ek_region[["Endkunde_NR", "GROSSREGION"]]
    .merge(yr_clust_scoring.explode(column="main_cls"), how="inner", on="Endkunde_NR")
    .rename(columns={"main_cls": "Clust"})
)


# In[ ]:


plt.figure(figsize=(16, 12))
sns.heatmap(
    data=normalize_rows(
        pd.crosstab(columns=GrReg_Clust.Clust, index=GrReg_Clust.GROSSREGION)
    ),
    annot=pd.crosstab(columns=GrReg_Clust.Clust, index=GrReg_Clust.GROSSREGION),
    fmt="d",
)


# # Cluster Scoring vs. Region

# In[ ]:


Reg_Clust = (
    ek_region[["Endkunde_NR", "REGION"]]
    .merge(yr_clust_scoring.explode(column="main_cls"), how="inner", on="Endkunde_NR")
    .rename(columns={"main_cls": "Clust"})
)


# In[ ]:


plt.figure(figsize=(16, 32))
sns.heatmap(
    data=normalize_rows(pd.crosstab(columns=Reg_Clust.Clust, index=Reg_Clust.REGION)),
    annot=pd.crosstab(columns=Reg_Clust.Clust, index=Reg_Clust.REGION),
    fmt="d",
)


# # Cluster Scoring vs. Netto-Umsatz

# In[ ]:


Netto_Clust = (
    ek_info_current[["Endkunde_NR", "netto_cat"]]
    .merge(yr_clust_scoring.explode(column="main_cls"), how="inner", on="Endkunde_NR")
    .rename(columns={"main_cls": "Clust"})
)


# In[ ]:


plt.figure(figsize=(16, 8))
sns.heatmap(
    data=normalize_rows(
        pd.crosstab(columns=Netto_Clust.Clust, index=Netto_Clust.netto_cat)
    ),
    annot=pd.crosstab(columns=Netto_Clust.Clust, index=Netto_Clust.netto_cat),
    fmt="d",
)


# # Map cluster scorings to periods, where appropriate

# ## Years, clustered by prc

# In[ ]:


ek_clusters = (
    pd.DataFrame.from_dict({"prc": bd_res_prc_2w_data_stack})
    .reset_index()
    .merge(
        pd.Series(
            data=yr_prc_nkmeans_labels,
            index=bd_res_prc_2w_data.index,
            name="yr_prc_clust",
        ),
        on=["Endkunde_NR", "Jahr"],
    )
)

ek_clusters.head()
