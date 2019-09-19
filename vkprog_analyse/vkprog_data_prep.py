#!/usr/bin/env python
# coding: utf-8


##################
## Load Modules ##
##################

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#matplotlib inline
import sklearn
import seaborn as sns

#######################
## Datenaufbereitung ##
#######################

## Libraries & Settings ##

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

from pa_lib.file import data_files, load_bin, store_bin, load_csv, write_xlsx, load_xlsx
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

###################
## Load raw data ##
###################

bd_raw = load_bin("vkprog\\bd_data.feather").rename(
    mapper=lambda name: cap_words(name, sep="_"), axis="columns"
)
bd = bd_raw.loc[(bd_raw.Netto > 0)].pipe(clean_up_categoricals)


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
    
    # Needed for data preparation
    df_aggr.eval("YYYYKW_2 = Jahr * 100 + KW_2", inplace=True)
    
    info("aggregate_bookings: Done.")
    return df_aggr


## Apply functions ##

bd_aggr_2w = aggregate_bookings(bd, 'KW_2')

######################
## Global Variables ##
######################

import datetime as dt
from dateutil.relativedelta import relativedelta

#date_now      = dt.datetime.now() # only works for odd calendar weeks!!!
date_now      = dt.datetime(2019,9,23) # only works for odd calendar weeks!!!

date_training = date_now - relativedelta(years=1) 
number_years  = 4 # how many years should be featured

current_year_kw_day = date_now.isocalendar()
current_yyyykw = current_year_kw_day[0]*100+current_year_kw_day[1] # Current calender week in format: YYYYKW

global_variables = dict({"date_now": date_now, 
                         "current_year_kw_day": current_year_kw_day,
                         "current_yyyykw"     : current_yyyykw,
                         "number_years"       : number_years,
                         "date_training"      : date_training}
                       )

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
    info("Computing: Yearly total sums")
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
        info("Merging: Left-Join to Container dataframe")
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
    KW = "KW_"+str(int(YYYYKW- np.floor(YYYYKW/100)*100))
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


## Apply functions ##

print("Creating: scoring_bd")
scoring_bd  = booking_data(current_yyyykw,4)

print("Creating: training_bd")
training_bd = booking_data(current_yyyykw-100,4)

# Check if both tables have the same columns names
print("[", list(scoring_bd.columns) == list(training_bd.columns), "] Both sets have same columns")

# Show me the first few lines
print("training_bd:")
training_bd.head(4)

#########################
## Set feature columns ##
#########################

feature_colnames_bd = list(training_bd.columns)   
# Don't scale the following columns:
feature_colnames_bd.remove("Endkunde_NR")
feature_colnames_bd.remove("Target_Aus_flg")
feature_colnames_bd.remove("Target_Res_flg")

display(feature_colnames_bd)


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
             .apply(lambda x: x + relativedelta(weeks=2) < view_date),:])
    
    return final_selection


## Apply function ##


print("Creating: training_dates")
training_dates = dates_bd(date_training)
print("Creating: scoring_dates")
scoring_dates  = dates_bd(date_now)

# Check if both tables have the same columns names
print("[", list(scoring_dates.columns) == list(training_dates.columns), "] Both sets have same columns")

##  Store date-feature names in a list ##

feature_colnames_dates = list(training_dates.columns)
feature_colnames_dates.remove("Endkunde_NR")
feature_colnames_dates.remove("Kampagne_Erfass_Datum_min")
feature_colnames_dates.remove("Kampagne_Erfass_Datum_max")
print(feature_colnames_dates)

####################
## Merge Datasets ##
####################

# 1. Reservation Dates
# 2. Booking data

# <div class="alert alert-block alert-info">
# <b>Remark:</b> Merge via INNER-JOIN, to apply all necessary filtration criteria.
# </div>

training_all = pd.merge(training_dates,training_bd,on="Endkunde_NR", how="inner")
scoring_all  = pd.merge(scoring_dates,  scoring_bd, on="Endkunde_NR", how="inner")

# Check if both tables have the same columns names
print("[", list(scoring_all.columns) == list(training_all.columns), "] Both sets have same columns")


# ## Scale Data

# <div class="alert alert-block alert-info">
# <b>Remark:</b> Scaling has to take place after all filtrations have taken place!
# </div>

# In[48]:


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
        dataset[x] = (logtransformed-min_)/(max_-min_) # standardise into floats in [0,1]
    
    for x in col_dates:
        transformed = dataset.loc[:,x]
        min_ = np.min(transformed)
        max_ = np.max(transformed)
        dataset[x] = (transformed-min_)/ (max_-min_)  
        
    
    return dataset


## Apply functions ##


scaled_training_all = scaling_bd(training_all,col_bookings=feature_colnames_bd, col_dates=feature_colnames_dates)
scaled_scoring_all  = scaling_bd(scoring_all, col_bookings=feature_colnames_bd, col_dates=feature_colnames_dates)

scaled_training_all.to_csv("C:\\Users\\stc\\data\\scaled_training_all.csv")
