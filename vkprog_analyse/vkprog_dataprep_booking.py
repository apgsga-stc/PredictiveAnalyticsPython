#!/usr/bin/env python
# coding: utf-8


##################
## Load Modules ##
##################

import datetime as dt

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

#######################
## Datenaufbereitung ##
#######################

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

## Libraries & Settings ##
from pa_lib.file import load_bin
from pa_lib.util import cap_words, iso_to_datetime, flat_list
from pa_lib.log import info, warn

from pa_lib.data import unfactorize, clean_up_categoricals

## Lazy Recursive Job Dependency Request:
from pa_lib.job import request_job


bd = pd.DataFrame()
bd_aggr_2w = pd.DataFrame()

################################################################################
## Recursive Dependency Check:
request_job(job_name="bd_prepare.py", current="Today")  # output: bd_data.feather
################################################################################

###################
## Load raw data ##
###################
def load_booking_data():
    bd_raw = load_bin("vkprog/bd_data.feather").rename(
        mapper=lambda name: cap_words(name, sep="_"), axis="columns"
    )
    _bd_ = bd_raw.loc[(bd_raw.Netto > 0)].pipe(clean_up_categoricals)
    return _bd_


######################################
## Filter criteria defined by Sales ##
######################################


def filter_dataset(dataframe):
    container_df = (
        dataframe.query("Dauer < 62")
        .query(
            'Auftragsart != ["Eigenwerbung APG|SGA", "Aushangauftrag Partner", "Logistik für Dritte", "Politisch"]'
        )
        .query('not Segment == "Airport" and not KV_Typ == "KPGL"')
        .pipe(clean_up_categoricals)
        .reset_index(drop=True)
    )
    return container_df


######################################################
## Booking Data (Beträge: Reservationen & Aushänge) ##
######################################################


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
    df_res = sum_calc(df, "Kamp_Erfass_Jahr", f"Kamp_Erfass_{period}")

    df_aus = df.copy().loc[df.Kamp_Beginn_Jahr.notnull()]
    if df.shape[0] != df_aus.shape[0]:
        warn(
            f"Kampagnen ohne Aushangdatum gelöscht: {df.shape[0] - df_aus.shape[0]} Einträge"
        )
    df_aus = sum_calc(df_aus, "Kamp_Beginn_Jahr", f"Kamp_Beginn_{period}")

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
        .astype({"Jahr": "int64"})  # .astype({"Jahr": "float"})
        .astype({period: "int8"})
        .sort_values(["Jahr", "Endkunde_NR", period])
        .reset_index(drop=True)
    )

    # Needed for data preparation
    df_aggr.eval("YYYYKW_2 = Jahr * 100 + KW_2", inplace=True)
    return df_aggr


######################
## Global Variables ##
######################


#############################
## Data Prep: Booking Data ##
#############################

## Functions ##


def booking_yearly_totals(yyyykw, year_span):
    """
    Computing yearly totals for Aushang and Reservation.
    Warning: Yearly totals do not necessarily align with calendar years!
    """
    # info("Starting: booking_yearly_totals")
    container_df = pd.DataFrame()
    container_df.loc[:, "Endkunde_NR"] = pd.Series(
        list(set(bd_aggr_2w.loc[:, "Endkunde_NR"]))
    )
    # bd_aggr_2w.eval("YYYYKW_2 = Jahr * 100 + KW_2", inplace=True)

    # info("Computing: Yearly total sums")
    for ry in list(range(year_span)):
        # bd_aggr_2w.loc[:,"YYYYKW_2"] = bd_aggr_2w.Jahr.map(lambda x: x*100) + bd_aggr_2w.KW_2

        bd_filtered = bd_aggr_2w.loc[
            (
                (bd_aggr_2w.loc[:, "YYYYKW_2"] < yyyykw - 100 * ry)
                & (bd_aggr_2w.loc[:, "YYYYKW_2"] >= yyyykw - 100 * (1 + ry))
            ),
            :,
        ].copy()

        bd_filtered.loc[:, "Year_Total"] = "_RY_" + str(ry)

        bd_pivot = bd_filtered.pivot_table(
            # index=["Endkunde_NR", "Jahr"],
            index=["Endkunde_NR"],
            # columns="KW_2",
            columns=["Year_Total"],
            values=[
                "Netto_Sum_Res",
                "Netto_Sum_Aus",
            ],  # Cash amount of Resevation placed per in weeks of YYYYKW and YYYY(KW+1)
            aggfunc="sum",
            fill_value=0,
            # There's a difference between 0 and NaN. Consider 0 only when the customer has had a real booking or reservation prior.
        )

        # Flatten down dataframe:
        bd_flattened = pd.DataFrame(bd_pivot.to_records(index=False))

        # Re-add column with Endkunde_NR
        bd_flattened.loc[:, "Endkunde_NR"] = pd.Series(bd_pivot.index)

        # Renaming column names:
        bd_flattened.columns = [
            x.replace("', '", "")
            .replace("', ", "")
            .replace("('", "")
            .replace(")", "")
            .replace("'", "")
            for x in bd_flattened.columns
        ]

        # Left-Join to the container
        # info("Merging: Left-Join to Container dataframe")
        container_df = pd.merge(
            container_df, bd_flattened, on="Endkunde_NR", how="left"
        )

    # Replace all NaN with Zero
    container_df.fillna(0, inplace=True)

    return container_df


##


def booking_data(yyyykw, year_span):
    """
    Creates pivot table for time span between YYYYKW and back the selected amount of years year_span
    """
    # Select the last four years based on new reference-column
    row_select = (bd_aggr_2w.loc[:, "YYYYKW_2"] <= yyyykw) & (
        bd_aggr_2w.loc[:, "YYYYKW_2"] >= yyyykw - year_span * 100
    )
    bd_filtered = bd_aggr_2w.loc[row_select, :].copy()

    # Create new column containing names of the relative years:
    # pd.options.mode.chained_assignment = None  # default='warn'
    max_jahr = yyyykw // 100
    bd_filtered.loc[:, "Jahr_relative"] = (
        "_RY_"
        + (max_jahr - bd_filtered.loc[:, "Jahr"]).astype("int8").astype("str")
        + "_KW_"
    )
    # pd.options.mode.chained_assignment = 'warn'  # default='warn'

    # Computing Sums for each kw and customer
    # info("Computing: Pivot Table")
    bd_pivot = bd_filtered.pivot_table(
        index=["Endkunde_NR"],
        columns=["Jahr_relative", "KW_2"],
        values=[
            "Netto_Sum_Res",
            "Netto_Sum_Aus",
        ],  # Cash amount of Resevation placed per in weeks of YYYYKW and YYYY(kw+1)
        aggfunc="sum",
        fill_value=0,
        # There's a difference between 0 and NaN. Consider 0 only when the customer has had a real booking or reservation prior.
    )

    # Flatten down dataframe
    bd_flattened = pd.DataFrame(bd_pivot.to_records(index=False))

    # Read column with Endkunde
    bd_flattened.loc[:, "Endkunde_NR"] = pd.Series(bd_pivot.index)

    # Renaming column names:
    bd_flattened.columns = [
        x.replace("', '", "").replace("', ", "").replace("('", "").replace(")", "")
        for x in bd_flattened.columns
    ]

    # Label target variables:
    kw = "KW_" + str(int(yyyykw - (yyyykw // 100) * 100))
    bd_flattened.rename(
        columns={
            "Netto_Sum_Res_RY_0_" + kw: "Target_Sum_Res_RY_0_" + kw,
            "Netto_Sum_Aus_RY_0_" + kw: "Target_Sum_Aus_RY_0_" + kw,
        },
        inplace=True,
    )

    bd_flattened.loc[:, "Target_Res_flg"] = bd_flattened.loc[
        :, "Target_Sum_Res_RY_0_" + kw
    ].astype(
        "bool"
    )  # Reservation?: Yes/No - True/False

    bd_flattened.loc[:, "Target_Aus_flg"] = bd_flattened.loc[
        :, "Target_Sum_Aus_RY_0_" + kw
    ].astype(
        "bool"
    )  # Aushang?: Yes/No - True/False

    # Sort index
    bd_flattened.sort_index(axis=1, inplace=True)

    # Compute yearly totals
    # info("Running: booking_yearly_totals(YYYYKW, year_span) ")
    yearly_totals = booking_yearly_totals(yyyykw, year_span)

    # Left join yearly totals, and return it
    # info("Final merge")
    return pd.merge(bd_flattened, yearly_totals, on="Endkunde_NR", how="left")


#######################
## Reservation Dates ##
#######################

# 1. Erste Reservation muss vor _View Date_ liegen
# 2. Die letzte Reservation vor dem _View Date_ darf nicht in den letzten 2 Wochen liegen. Sehr unwahrscheinlich, dass diese gleich nochmal buchen, ausserdem  technische Vermeidung von Überlappungen
# 3. Muss Target Aussage haben. (Zur zeit Reservationen)


def dates_bd(view_date):
    """
    filter bd to campaigns before view_date, calculate deltas
    """
    sec_per_year = 60 * 60 * 24 * 365.25
    min_max_erfass_dt = (
        bd.loc[
            bd["Kampagne_Erfassungsdatum"] < view_date,
            ["Endkunde_NR", "Kampagne_Erfassungsdatum"],
        ]
        .groupby("Endkunde_NR", as_index=False)
        .agg(["min", "max"])
    ).reset_index()
    min_max_erfass_dt = pd.DataFrame(min_max_erfass_dt.to_records(index=False))
    min_max_erfass_dt.columns = [
        "Endkunde_NR",
        "Kampagne_Erfass_Datum_min",
        "Kampagne_Erfass_Datum_max",
    ]
    min_max_erfass_dt.loc[:, "Erste_Buchung_Delta"] = min_max_erfass_dt.loc[
        :, "Kampagne_Erfass_Datum_min"
    ].apply(lambda x: ((view_date - x).total_seconds()) // sec_per_year)
    min_max_erfass_dt.loc[:, "Letzte_Buchung_Delta"] = min_max_erfass_dt[
        "Kampagne_Erfass_Datum_max"
    ].apply(lambda x: (view_date - x).total_seconds() // sec_per_year)

    min_max_erfass_dt.loc[:, "Erste_Letzte_Buchung_Delta"] = (
        min_max_erfass_dt.loc[:, "Erste_Buchung_Delta"]
        - min_max_erfass_dt.loc[:, "Letzte_Buchung_Delta"]
    )
    return min_max_erfass_dt


##############
## Branchen ##
##############
def branchen_data(view_date: date):
    # Prepare data
    kunden_branchen_df = bd.loc[
        :, ["Endkunde_NR", "Kampagne_Erfassungsdatum", "Endkunde_Branchengruppe_ID"]
    ]
    auftrag_branchen_df = bd.loc[
        :, ["Endkunde_NR", "Kampagne_Erfassungsdatum", "Auftrag_Branchengruppe_ID"]
    ]
    kunden_branchen_df.columns = [
        "Endkunde_NR",
        "Kampagne_Erfassungsdatum",
        "Branchen_ID",
    ]
    auftrag_branchen_df.columns = kunden_branchen_df.columns
    branchen_df = (
        kunden_branchen_df.append(auftrag_branchen_df).dropna().drop_duplicates()
    )

    # TODO: (pd.crosstab(index=df.nr, columns=df.br) > 0).astype("int")

    # Keep past campaigns only, count Branchen_ID occurrences per customer:
    branchen_df = branchen_df.loc[
        bd["Kampagne_Erfassungsdatum"] < view_date
    ].pivot_table(
        index=["Endkunde_NR"],
        columns=["Branchen_ID"],
        values=["Branchen_ID"],
        aggfunc="count",
        fill_value=0,
    )

    # Flatten and rename:
    branchen_df = pd.DataFrame(branchen_df.to_records(index=True))
    new_col_names = [
        x.replace("('Kampagne_Erfassungsdatum',", "B")
        .replace(" '", "")
        .replace("')", "")
        for x in branchen_df.columns
    ]
    branchen_df.columns = new_col_names

    # Decode into 0/1:
    for branche in branchen_df.columns[1:]:
        branchen_df.loc[:, branche] = branchen_df.loc[:, branche].apply(
            lambda x: min(x, 1)
        )

    return branchen_df


################
## Scale Data ##
################


def scaling_bd(dataset, col_bookings, col_dates):
    """
    Booking columns are heavily right-skewed:
     1. log-transform all columns => achieving approx. gaussian distribution
     2. Standardise log-transformed values into interval [0,1]

    Return transformed dataframe
    """
    # Scaling: booking
    for x in col_bookings:
        logtransformed = np.log(
            dataset.loc[:, x] + 1
        )  # bookings are heavily right-skewed. log-transform to get approx. gaussian distribution
        min_ = np.min(logtransformed)
        max_ = np.max(logtransformed)
        if min_ != max_:
            dataset[x] = (logtransformed - min_) / (
                max_ - min_
            )  # standardise into floats in [0,1]
        else:
            dataset[x] = 0

    for x in col_dates:
        transformed = dataset.loc[:, x]
        min_ = np.min(transformed)
        max_ = np.max(transformed)
        if min_ != max_:
            dataset[x] = (transformed - min_) / (max_ - min_)
        else:
            dataset[x] = 0

    return dataset


########################################################################################


def remove_list(input_list: list, to_remove: Any) -> list:
    """
    Remove a list of elements from another list, if present

    :param input_list: source list
    :param to_remove: element(s) to remove
    :return: remaining elements, in original order
    """
    rest = [x for x in input_list if not x in flat_list(to_remove)]
    return rest


##


def bd_train_scoring(
    day: int,
    month: int,
    year_score: int,
    year_train: int,
    year_span: int,
    sales_filter: bool,
    scale_features: bool,
):
    """
    Creates scoring-dataset, training-dataset, feature columns name lists for bookings and booking-dates
    """
    global bd, bd_aggr_2w

    ## Load data, aggregate and filter according to parameters
    date_now = dt.datetime(
        year_score, month, day
    )  # only works for odd calendar weeks!!!
    kw_now = date_now.isocalendar()[1]
    date_training = iso_to_datetime(year=year_train, kw=kw_now, day=1)
    current_yyyykw = year_score * 100 + kw_now
    training_yyyykw = year_train * 100 + kw_now
    bd = load_booking_data()
    if sales_filter:
        bd = filter_dataset(bd)
        info("True: Filters applied, defined by Sales")
    else:
        info("False: Filters applied, defined by Sales")
    bd_aggr_2w = aggregate_bookings(bd, "KW_2")
    info(f"(current_yyyykw / training_yyyykw): ({current_yyyykw} / {training_yyyykw})")
    info(f"(date_now / date_training): ({date_now} / {date_training})")
    scoring_bd = booking_data(current_yyyykw, year_span)
    training_bd = booking_data(training_yyyykw, year_span)
    feature_colnames_bd = remove_list(
        input_list=list(training_bd.columns),
        to_remove=["Endkunde_NR", "Target_Aus_flg", "Target_Res_flg"],
    )
    ## Relative Dates
    training_dates = dates_bd(date_training)
    scoring_dates = dates_bd(date_now)
    feature_colnames_dates = remove_list(
        input_list=list(training_dates.columns),
        to_remove=[
            "Endkunde_NR",
            "Kampagne_Erfass_Datum_min",
            "Kampagne_Erfass_Datum_max",
        ],
    )

    ## Branchen:
    training_branchen = branchen_data(date_training)
    scoring_branchen = branchen_data(date_now)
    feature_colnames_branchen = list(
        set(training_branchen.columns) & set(scoring_branchen.columns)
    )
    feature_colnames_branchen.sort()
    feature_colnames_branchen.remove("Endkunde_NR")

    ## Merge all data
    training_all = pd.merge(training_dates, training_bd, on="Endkunde_NR")
    training_all = pd.merge(
        training_all, training_branchen, on="Endkunde_NR", how="left"
    )
    scoring_all = pd.merge(scoring_dates, scoring_bd, on="Endkunde_NR")
    scoring_all = pd.merge(scoring_all, scoring_branchen, on="Endkunde_NR", how="left")

    ## Scale features, if requested
    if scale_features:
        info("Scaling features")
        training_all = scaling_bd(
            training_all,
            col_bookings=feature_colnames_bd,
            col_dates=feature_colnames_dates,
        )
        scoring_all = scaling_bd(
            scoring_all,
            col_bookings=feature_colnames_bd,
            col_dates=feature_colnames_dates,
        )
    else:
        info("Unscaled features")

    return (
        training_all,
        scoring_all,
        feature_colnames_bd,
        feature_colnames_dates,
        feature_colnames_branchen,
    )


#####################################################################
