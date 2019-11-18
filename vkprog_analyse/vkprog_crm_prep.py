#!/usr/bin/env python
# coding: utf-8

# # Data-Prep: CRM Data

###############
## Load libs ##
###############

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

from pa_lib.log import time_log, info
import pandas as pd
pd.options.display.max_columns = None


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
)
from pa_lib.data import desc_col
from pa_lib.data import boxplot_histogram
import numpy as np
from functools import reduce

## Lazy Recursive Job Dependency Request:
from pa_lib.job import request_job

################################################################################
## Recursive Dependency Check:
request_job(job_name="crm_prepare.py", current= "Today") # output: bd_data.feather

################################################################################
###################
## Load CRM data ##
###################

def load_crm_data():
    raw_data = load_bin("vkprog\\crm_data_vkprog.feather").rename(
        mapper=lambda name: cap_words(name, sep="_"), axis="columns"
    )
    #return raw_data.astype({"Endkunde_NR": "int64", "Year": "int64", "KW_2": "int64"})
    return raw_data.astype({"Year": "int64", "KW_2": "int64"})

####################################################
## Yearly aggregation per ``Kanal`` group element ##
####################################################

def contacts_grouped_yrly(date_view,kanal_grps,year_span):
    ####
    def yrl_kanal_contacts(date_view, group_name, rel_year):
        return (raw_crm_data.loc[(raw_crm_data.loc[:,"Kanal"].isin(kanal_grps[group_name]) &     # adjust to key
                         (raw_crm_data.loc[:,"Datum"] <  date_view  - relativedelta(years= rel_year   )) &
                         (raw_crm_data.loc[:,"Datum"] >= date_view  - relativedelta(years= rel_year+1 )) # adjust years
                         ),:].groupby("Endkunde_NR").count()
                             .reset_index(inplace=False)
                             .loc[:,["Endkunde_NR","Kanal"]]
                             .rename(columns={"Kanal": f"RY_{rel_year}_Anz_{group_name}"})   # adjust "Anzahl"
                             #.sort_values("Anzahl", ascending=False)
               )
    #####
    container_df = yrl_kanal_contacts(date_view=date_view,
                                      group_name=list(kanal_grps.keys())[1],rel_year=0).loc[:,"Endkunde_NR"]
    for name in kanal_grps.keys():
        for i in range(year_span):
            rel = yrl_kanal_contacts(date_view=date_view,
                                     group_name=name,
                                     rel_year=i)
            container_df = pd.merge(container_df,
                                    rel,
                                    on="Endkunde_NR",
                                    how="inner")
    return container_df

####################################
## Delta(view_date, last_contact) ##
####################################

def delta_contact(date_view,kanal_grps):
    for name in kanal_grps.keys():
        raw_crm_data.loc[raw_crm_data.Kanal.isin(kanal_grps[name]), "Kanal_Grps"] = name
    
    max_vertical_df = (raw_crm_data.loc[(raw_crm_data.loc[:,"Datum"] <  date_view) # adjust years
                     ,:]
                .groupby(["Endkunde_NR", "Kanal_Grps"])
                .agg({"Datum": np.max})
                .reset_index(inplace=False)
                   )
    max_vertical_df["delta_days"] = (date_view - max_vertical_df.loc[:,"Datum"]).apply(lambda x: x.total_seconds()) / 86400  # delta in days
    
    flatten_df = max_vertical_df.pivot_table(
        index      = "Endkunde_NR",
        columns    = ["Kanal_Grps"],
        values     = ["delta_days"],
        aggfunc    = "min",
        #fill_value = np.inf # Do not fill them!
    
    ).reset_index(inplace=False)
    
    flatten_df  = pd.DataFrame(flatten_df.to_records(index=False))
    flatten_df.columns = ["Endkunde_NR"]+[ "Letzter_Kontakt_Delta_"+x.replace("'","").replace("(","").replace("delta_days, ","").replace(",","").replace(" ","").replace(")","") for x in flatten_df.columns[1:]]
    
    flatten_df["Letzter_Kontakt_Delta_global"] = flatten_df.iloc[:,1:].min(axis = 1, skipna = True)
    
    #Letzte_Buchung_Delta
    
    return flatten_df

#######################################################
## Wrapper Function, that does everything in one go! ##
#######################################################

def crm_train_scoring(day, month, year_score, year_train, year_span):
    info("Start.")
    date_now      = dt.datetime(year_score,month,day) # only works for odd calendar weeks!!!
    date_training = dt.datetime(year_train,month,day) # only works for odd calendar weeks!!!
    
    date_training = iso_to_datetime(year= year_train,
                                   kw   = date_now.isocalendar()[1],
                                   day  = 1
                                   )
    
    global raw_crm_data
    raw_crm_data = load_crm_data()
    
    ## Define groups for Kanal ##
    all_kanal =set(raw_crm_data.loc[:,"Kanal"])
    kanal_grps = {}

    kanal_grps["Besprechung"]         = {"Besprechung"}
    kanal_grps["Besuch"]              = {"Besuch"}
    kanal_grps["Brief_Dankeskarte"]   = {"Brief","Dankeskarte"}
    kanal_grps["E-Mail"]              = {"E-Mail"}
    kanal_grps["Event_Veranstaltung"] = {"Event","Veranstaltung"}
    kanal_grps["Telefon"]             = {"Telefon"}

    # Stuff all the rest into "Anderes":
    kanal_grps["Anderes"]             =  all_kanal - reduce(set.union,kanal_grps.values()) 
    ## End of definition
    
    def crm_prep(date_view,year_span):
        last_contacts_df      = delta_contact(date_view=date_view,
                                              kanal_grps=kanal_grps)
        grpd_yrly_contacts_df = contacts_grouped_yrly(date_view=date_view,
                                                      kanal_grps=kanal_grps,
                                                      year_span=year_span)

        return pd.merge(grpd_yrly_contacts_df,last_contacts_df,on="Endkunde_NR",how="inner").astype({"Endkunde_NR" : "int64"})
    
    crm_train_df = crm_prep(date_view=date_training, year_span=year_span)
    crm_score_df = crm_prep(date_view=date_now,      year_span=year_span)
    
    feature_colnames_crm = list(crm_train_df.columns[1:])
    
    info("Finished.")
    return (crm_train_df, crm_score_df, feature_colnames_crm)


##################
## End of file. ##
##################