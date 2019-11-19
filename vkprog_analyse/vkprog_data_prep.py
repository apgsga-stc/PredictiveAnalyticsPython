#!/usr/bin/env python
# coding: utf-8


##################
## Load Modules ##
##################

import numpy as np
import pandas as pd

#######################
## Datenaufbereitung ##
#######################


# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))



## Libraries & Settings ##
from pa_lib.file import load_bin
from pa_lib.util import (
    cap_words,
    iso_to_datetime
    )
from pa_lib.log import time_log, info

import datetime as dt
from dateutil.relativedelta import relativedelta

from pa_lib.data import (
    clean_up_categoricals,
    unfactorize,
    clean_up_categoricals
)

## Lazy Recursive Job Dependency Request:
from pa_lib.job import request_job

################################################################################
## Recursive Dependency Check:
request_job(job_name="bd_prepare.py",  current= "Today") # output: bd_data.feather

################################################################################

###################
## Load raw data ##
###################
def load_booking_data():
    bd_raw = load_bin("vkprog\\bd_data.feather").rename(
        mapper=lambda name: cap_words(name, sep="_"), axis="columns"
    )
    bd = bd_raw.loc[(bd_raw.Netto > 0)].pipe(clean_up_categoricals)
    return bd

######################################
## Filter criteria defined by Sales ##
######################################

def filter_dataset(dataframe):
    container_df = (dataframe.query("Dauer < 62")
                 .query('Auftragsart != ["Eigenwerbung APG|SGA", "Aushangauftrag Partner", "Logistik für Dritte", "Politisch"]')
                 .query('not Segment == "Airport" and not KV_Typ == "KPGL"')
                 .pipe(clean_up_categoricals)
                 .reset_index(drop=True)
                )
    return container_df

######################################################
## Booking Data (Beträge: Reservationen & Aushänge) ##
######################################################

def sum_calc(df, col_year, col_week, keine_annulierten=True):
    if keine_annulierten == True:
        filter_spalte = pd.Series(df.loc[:,"Kampagnen_Status"] != 3)
    else:
        filter_spalte = pd.Series(df.loc[:,"Kampagnen_Status"] != 99999999999999 ) # Series of "True", no filter.
        
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
    df_res = sum_calc(df, "Kamp_Erfass_Jahr", f"Kamp_Erfass_{period}",keine_annulierten=True)
    info("Calculate Aushang...")
    df_aus = sum_calc(df, "Kamp_Beginn_Jahr", f"Kamp_Beginn_{period}",keine_annulierten=True)

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
    
    # Needed for data preparation
    df_aggr.eval("YYYYKW_2 = Jahr * 100 + KW_2", inplace=True)
    
    info("aggregate_bookings: Done.")
    return df_aggr

######################
## Global Variables ##
######################

import datetime as dt
from dateutil.relativedelta import relativedelta

#############################
## Data Prep: Booking Data ##
#############################

## Functions ##

def booking_yearly_totals(YYYYKW, year_span):
    """
    Computing yearly totals for Aushang and Reservation.
    Warning: Yearly totals do not necessarily align with calendar years!
    """
    #info("Starting: booking_yearly_totals")
    container_df = pd.DataFrame()
    container_df.loc[:,"Endkunde_NR"] =pd.Series(list(set(bd_aggr_2w.loc[:,"Endkunde_NR"])))
    #bd_aggr_2w.eval("YYYYKW_2 = Jahr * 100 + KW_2", inplace=True)
    
    #info("Computing: Yearly total sums")
    for ry in list(range(year_span)):
        #bd_aggr_2w.loc[:,"YYYYKW_2"] = bd_aggr_2w.Jahr.map(lambda x: x*100) + bd_aggr_2w.KW_2
                
        bd_filtered = bd_aggr_2w.loc[((bd_aggr_2w.loc[:,"YYYYKW_2"] <  YYYYKW-100*ry) &
                                      (bd_aggr_2w.loc[:,"YYYYKW_2"] >= YYYYKW-100*(1+ry))),:].copy()
        
        bd_filtered.loc[:,"Year_Total"] = "_RY_"+str(ry)
        
        bd_pivot = bd_filtered.pivot_table(
            #index=["Endkunde_NR", "Jahr"],
            index=["Endkunde_NR"],
            #columns="KW_2",
            columns = ["Year_Total"],
            values=["Netto_Sum_Res","Netto_Sum_Aus"], # Cash amount of Resevation placed per in weeks of YYYYKW and YYYY(KW+1)
            aggfunc="sum",
            fill_value=0, # There's a difference between 0 and NaN. Consider 0 only when the customer has had a real booking or reservation prior.
        )
        
        # Flatten down dataframe:
        bd_flattened = pd.DataFrame(bd_pivot.to_records(index=False))
        
        # Re-add column with Endkunde_NR
        bd_flattened.loc[:,"Endkunde_NR"] = pd.Series(bd_pivot.index)
        
        # Renaming column names:
        bd_flattened.columns = [x.replace("', '",'')
                                .replace("', ",'')
                                .replace("('","")
                                .replace(")","")
                                .replace("'","") for x in bd_flattened.columns]
        
        # Left-Join to the container
        #info("Merging: Left-Join to Container dataframe")
        container_df = pd.merge(container_df, bd_flattened, on="Endkunde_NR", how="left")
    
    # Replace all NaN with Zero
    container_df.fillna(0, inplace=True)
    
    return container_df

##

def booking_data(YYYYKW, year_span):
    """
    Creates pivot table for time span between YYYYKW and back the selected amount of years year_span
    """
    # Select the last four years based on new reference-column
    bd_filtered = bd_aggr_2w.loc[((bd_aggr_2w.loc[:,"YYYYKW_2"] <= YYYYKW) &
                                  (bd_aggr_2w.loc[:,"YYYYKW_2"] >=  YYYYKW-year_span*100)),:].copy()
    
    # Create new column containing names of the relative years: 
    #pd.options.mode.chained_assignment = None  # default='warn'
    max_Jahr = YYYYKW//100
    bd_filtered.loc[:,"Jahr_relative"] = "_RY_"+(max_Jahr-bd_filtered.loc[:,"Jahr"]).astype('str')+"_KW_"
    #pd.options.mode.chained_assignment = 'warn'  # default='warn'
    
    # Computing Sums for each KW and customer
    info("Computing: Pivot Table")
    bd_pivot    = bd_filtered.pivot_table(
        #index=["Endkunde_NR", "Jahr"],
        index=["Endkunde_NR"],
        #columns="KW_2",
        columns = ["Jahr_relative","KW_2"],
        values=["Netto_Sum_Res","Netto_Sum_Aus"] , # Cash amount of Resevation placed per in weeks of YYYYKW and YYYY(KW+1)
        aggfunc="sum",
        fill_value=0, # There's a difference between 0 and NaN. Consider 0 only when the customer has had a real booking or reservation prior.
    )
    # Flatten down dataframe
    bd_flattened = pd.DataFrame(bd_pivot.to_records(index=False))
    
    # Read column with Endkunde
    bd_flattened.loc[:,"Endkunde_NR"] = pd.Series(bd_pivot.index)
    
    # Renaming column names:
    bd_flattened.columns = [x.replace("', '",'')
                            .replace("', ",'')
                            .replace("('","")
                            .replace(")","") for x in bd_flattened.columns]
    
    # Label target variables:
    KW = "KW_"+str(int(YYYYKW- (YYYYKW//100)*100))
    bd_flattened.rename(columns={"Netto_Sum_Res_RY_0_"+KW: "Target_Sum_Res_RY_0_"+KW,
                                 "Netto_Sum_Aus_RY_0_"+KW: "Target_Sum_Aus_RY_0_"+KW},
                        inplace=True)
    bd_flattened.loc[:,"Target_Res_flg"] = bd_flattened.loc[:,"Target_Sum_Res_RY_0_"+KW].astype('bool') # Reservation?: Yes/No - True/False
    bd_flattened.loc[:,"Target_Aus_flg"] = bd_flattened.loc[:,"Target_Sum_Aus_RY_0_"+KW].astype('bool') # Aushang?: Yes/No - True/False
    
    # Sort index
    bd_flattened.sort_index(axis=1, inplace=True)
    
    # Compute yearly totals
    info("Running: booking_yearly_totals(YYYYKW, year_span) ")
    yearly_totals = booking_yearly_totals(YYYYKW, year_span)
    
    # Left join yearly totals, and return it
    info("Final merge")
    return pd.merge(bd_flattened, yearly_totals, on="Endkunde_NR", how="left")


#######################
## Reservation Dates ##
#######################

# 1. Erste Reservation muss vor _View Date_ liegen
# 2. Die letzte Reservation vor dem _View Date_ darf nicht in den letzten 2 Wochen liegen. Sehr unwahrscheinlich, dass diese gleich nochmal buchen, ausserdem  technische Vermeidung von Überlappungen
# 3. Muss Target Aussage haben. (Zur zeit Reservationen)

def dates_bd(view_date):
    sec_kw2_factor = (60*60*24*365)
    min_max_erfass_dt = (
        bd.loc[view_date>bd.loc[:,"Kampagne_Erfassungsdatum"],["Endkunde_NR", "Kampagne_Erfassungsdatum"]]
          .groupby("Endkunde_NR")
          .agg(['min','max'])
          ).reset_index()

    min_max_erfass_dt = pd.DataFrame(min_max_erfass_dt.to_records(index=False))

    min_max_erfass_dt.columns = ["Endkunde_NR",
                                 "Kampagne_Erfass_Datum_min",
                                 "Kampagne_Erfass_Datum_max"]

    min_max_erfass_dt.loc[:,"Erste_Buchung_Delta"] = (
        min_max_erfass_dt
        .loc[:,"Kampagne_Erfass_Datum_min"]
        .apply(lambda x: ((view_date-x).total_seconds()) // sec_kw2_factor)
        #.fillna(-1)
        )

    min_max_erfass_dt.loc[:,"Letzte_Buchung_Delta"] = (
        min_max_erfass_dt
        .loc[:,"Kampagne_Erfass_Datum_max"]
        .apply(lambda x: (view_date-x).total_seconds() // sec_kw2_factor)
        #.fillna(-1)
        )
    
    min_max_erfass_dt.loc[:,"Erste_Letzte_Buchung_Delta"] = (
        min_max_erfass_dt.loc[:,"Erste_Buchung_Delta"] - min_max_erfass_dt.loc[:,"Letzte_Buchung_Delta"]
        )
    # Kick all customer, who just booked in the last two weeks
    final_selection = (
        min_max_erfass_dt
        .loc[min_max_erfass_dt
             .loc[:,"Kampagne_Erfass_Datum_max"]
             #.apply(lambda x: x + relativedelta(weeks=2) < view_date),:])
             .apply(lambda x: x <= view_date),:])
    
    return final_selection


##############
## Branchen ##
##############

def branchen_data(date_view):
    
    #bd = load_booking_data()
    
    kunden_branchen_df  = bd.loc[:,["Endkunde_NR",
                                    "Kampagne_Erfassungsdatum",
                                    "Endkunde_Branchengruppe_ID"] ]

    auftrag_branchen_df = bd.loc[:,["Endkunde_NR",
                                    "Kampagne_Erfassungsdatum",
                                    "Auftrag_Branchengruppe_ID"] ]

    kunden_branchen_df.columns  = ["Endkunde_NR",
                                   "Kampagne_Erfassungsdatum",
                                   "Branchen_ID"]
    
    auftrag_branchen_df.columns = kunden_branchen_df.columns

    branchen_df = (kunden_branchen_df.append(auftrag_branchen_df)
                                     .dropna()
                                     .drop_duplicates() )
    
    # Define filter (boolean pd.Series) based on view_date
    filter_boolean = (bd.loc[:,"Kampagne_Erfassungsdatum"] < date_view)
    
    # Span pivot table with filter:
    branchen_df = (branchen_df
                   .loc[filter_boolean,:]
                   .pivot_table(
                        index      = ["Endkunde_NR"],
                        columns    = ["Branchen_ID"],
                        values     = ["Branchen_ID"],
                        aggfunc    = "count",
                        fill_value = 0) )
    
    #Flatten and rename:
    branchen_df = pd.DataFrame(branchen_df.to_records(index=True))
    new_col_names = [x.replace("('Kampagne_Erfassungsdatum',","B").replace(" '","").replace("')","") for x in branchen_df.columns]
    branchen_df.columns = new_col_names
    
    #Decode into 0/1:
    for branche in branchen_df.columns[1:]:
        branchen_df.loc[:,branche] = branchen_df.loc[:,branche].apply(lambda x: min(x,1))
        
    return branchen_df


################
## Scale Data ##
################

def scaling_bd(dataset,col_bookings=[], col_dates=[]):
    """
    Booking columns are heavily right-skewed:
     1. log-transform all columns => achieving approx. gaussian distribution
     2. Standardise log-transformed values into interval [0,1]
     
    Return transformed dataframe
    """
    # Scaling: booking
    for x in col_bookings:
        logtransformed = np.log(dataset.loc[:,x]+1) # bookings are heavily right-skewed. log-transform to get approx. gaussian distribution
        min_ = np.min(logtransformed)
        max_ = np.max(logtransformed)
        if min_ != max_:
            dataset[x] = (logtransformed-min_)/(max_-min_) # standardise into floats in [0,1]
        else:
            dataset[x] = 0
    
    for x in col_dates:
        transformed = dataset.loc[:,x]
        min_ = np.min(transformed)
        max_ = np.max(transformed)
        if min_ != max_:
            dataset[x] = (transformed-min_) / (max_-min_)  
        else:
            dataset[x] = 0
        
    
    return dataset

########################################################################################################

def bd_train_scoring(day, month, year_score, year_train, year_span, sales_filter=True, scale_features = True) :
    """
    Creates scoring-dataset, training-dataset, feature columns name lists for bookings and booking-dates
    """
    
    date_now      = dt.datetime(year_score,month,day) # only works for odd calendar weeks!!!
    kw_now        = date_now.isocalendar()[1]
    
    date_training = iso_to_datetime(year=year_train,
                                    kw=kw_now,
                                    day=1)
    
    global current_yyyykw
    global training_yyyykw
    
    current_yyyykw  = year_score*100+kw_now
    training_yyyykw = year_train*100+kw_now
    
    global bd
    global bd_aggr_2w
    
    bd          = load_booking_data()
    if sales_filter == True:
        bd          = filter_dataset(bd)
        info("True: Filters applied, defined by Sales")
    else:
        info("False: Filters applied, defined by Sales")
        
    bd_aggr_2w  = aggregate_bookings(bd, 'KW_2')
    info(f"current_yyyykw: {current_yyyykw}")
    info(f"date_now:       {date_now}")
    info(f"training_yyyykw:{training_yyyykw}")
    info(f"date_training:  {date_training}")
    
    scoring_bd  = booking_data(current_yyyykw, year_span )
    training_bd = booking_data(training_yyyykw, year_span)

    ##  Store booking-feature names in a list ##
    feature_colnames_bd = list(training_bd.columns)   
    feature_colnames_bd.remove("Endkunde_NR")
    feature_colnames_bd.remove("Target_Aus_flg")
    feature_colnames_bd.remove("Target_Res_flg")
    
    ## Relative Dates
    training_dates = dates_bd(date_training)
    scoring_dates  = dates_bd(date_now)

    ##  Store date-feature names in a list ##
    feature_colnames_dates = list(training_dates.columns)
    feature_colnames_dates.remove("Endkunde_NR")
    feature_colnames_dates.remove("Kampagne_Erfass_Datum_min")
    feature_colnames_dates.remove("Kampagne_Erfass_Datum_max")
    
    ## Branchen:
    training_branchen = branchen_data(date_training)
    scoring_branchen  = branchen_data(date_now)
    
    ## Store branchen-feature names in a list:
    feature_colnames_branchen = list(set(training_branchen.columns) & set(scoring_branchen.columns))
    feature_colnames_branchen.sort()
    feature_colnames_branchen.remove("Endkunde_NR")
    
    
    ## Merge all data
    training_all = pd.merge(training_dates,
                            training_bd,
                            on="Endkunde_NR", how="inner")
    
    training_all = pd.merge(training_all,
                            training_branchen,
                            on="Endkunde_NR", how="left")
    
    scoring_all  = pd.merge(scoring_dates,
                            scoring_bd,
                            on="Endkunde_NR", how="inner")
    
    scoring_all  = pd.merge(scoring_all,
                            scoring_branchen,
                            on="Endkunde_NR", how="left")
    
    if scale_features == True:
        info("Scaling features")
        training_all = scaling_bd(training_all,col_bookings=feature_colnames_bd, col_dates=feature_colnames_dates)
        scoring_all  = scaling_bd(scoring_all, col_bookings=feature_colnames_bd, col_dates=feature_colnames_dates)
        
    else:
        info("Unscaled features")
        
    info("Finished.")
    
    return (training_all, scoring_all, feature_colnames_bd, feature_colnames_dates,feature_colnames_branchen)

#####################################################################

"""
(training_all, scoring_all, feature_colnames_bd, feature_colnames_dates) = bd_train_scoring(
    day=23,
    month=9,
    year_score=2019,
    year_train=2018,
    year_span=4,
    scale_features=True)


#check
training_all.head(3)
training_all.describe()
scoring_all.head(3)
feature_colnames_bd
feature_colnames_dates


#training_all.to_csv("C:\\Users\\stc\\data\\scaled_training_all.csv")

"""