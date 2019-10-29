# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
import numpy as np

from pa_lib.file import load_bin, store_bin, load_xlsx, project_dir
from pa_lib.data import clean_up_categoricals
from pa_lib.util import cap_words
from pa_lib.log import err, info

import copy

########################################################################################
def load_bookings():
    # get the raw data
    with project_dir("vkprog"):
        bookings_raw = load_bin("bd_data.feather").rename(
            mapper=lambda name: cap_words(name, sep="_"), axis="columns"
        )
    bookings = bookings_raw.loc[(bookings_raw.Netto > 0)].pipe(clean_up_categoricals)
    return bookings

########################################################################################
def load_plz():
    #get the raw data
    with project_dir("vkprog"):
        plz_data = load_bin("plz_data.feather")
        plz_data.loc[:,"PLZ"] = plz_data.loc[:,"PLZ"].astype("int64")
    return plz_data

########################################################################################
def load_crm():
    #get the raw data
    with project_dir("vkprog"):
        crm_data = load_bin("crm_data.feather").astype({"ENDKUNDE_NR": "int64"})
    return crm_data

########################################################################################
def collect(s, sep=","):
        return sep.join(map(str, s[s.notna()].unique()))

########################################################################################
def last_notna(s):
    try:
        return s.loc[s.notnull()].iat[-1]
    except IndexError:
        return np.NaN
    
########################################################################################
def aggregate_per_customer(bookings):
    """def last_notna(s):
        try:
            return s.loc[s.notnull()].iat[-1]
        except IndexError:
            return np.NaN

    def collect(s, sep=","):
        return sep.join(map(str, s[s.notna()].unique()))"""

    # this takes around 90 seconds
    customer_info = (
        bookings.sort_values(["Endkunde_NR", "Kampagne_Erfassungsdatum"])
        .astype({"Endkunde_NR": "int64", "Kamp_Erfass_Jahr": "int16"})
        .groupby("Endkunde_NR", as_index=False)
        .agg(
            {
                "Endkunde": last_notna,
                "EK_Aktiv": last_notna,
                "EK_Kam_Betreut": last_notna,
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
                "EK_HB_Apg_Kurzz":  last_notna, # added by STC
                "AG_Hauptbetreuer": last_notna, # added by STC
            }
        )
    )

    customer_info.set_axis(
        labels="""Endkunde_NR Endkunde EK_Aktiv EK_Kam_Betreut EK_Land EK_Plz EK_Ort Agentur EK_BG 
                  EK_BG_ID Auftrag_BG_ID Auftrag_BG_Anz Last_Res_Date First_Res_Year 
                  Last_Res_Year Last_Aus_Date EK_HB_Apg_Kurzz AG_Hauptbetreuer""".split(),
        axis="columns",
        inplace=True,
    )
    return customer_info


########################################################################################
def load_region_data():
    with project_dir("Raumgliederung"):
        plz_raw = load_xlsx("do-t-09.02-gwr-37.xlsx", sheet_name="PLZ4").rename(
            columns={
                "PLZ4": "PLZ",
                "%_IN_GDE": "PRC",
                "KTKZ": "KANTON",
                "GDENAMK": "GEMEINDE",
            }
        )
        region_raw = load_xlsx(
            "Raumgliederungen.xlsx", sheet_name="Daten", skiprows=0, header=1
        )
        reg_names = load_xlsx(
            "Raumgliederungen.xlsx",
            sheet_name="CH1+CL_BAE2018+1.0",
            skiprows=0,
            header=1,
            index_col=0,
        )
        grossreg_names = load_xlsx(
            "Raumgliederungen.xlsx",
            sheet_name="CH1+CL_GBAE2018+1.0",
            skiprows=0,
            header=1,
            index_col=0,
        )

    # make PLZ mapping unique (best match by max PRC)
    def main_features(df):
        return df.loc[df.PRC.idxmax(), ["PLZ", "KANTON", "GDENR", "GEMEINDE"]]

    plz_unique = plz_raw.groupby("PLZ", as_index=False).apply(main_features)
    name_unique = plz_raw.groupby("GEMEINDE", as_index=False).apply(main_features)
    plz_name_unique = (
        pd.concat([plz_unique, name_unique]).drop_duplicates().reset_index(drop=True)
    )

    # clean up reg info
    region_info = (
        region_raw.drop(
            index=0, columns=["Bezirks-nummer", "Kantons-nummer", "Gemeindename"]
        )
        .rename(
            columns={
                "BFS Gde-nummer": "GDENR",
                "Kanton": "KANTON",
                "Bezirksname": "BEZIRK",
                "Arbeitsmarktgrossregionen 2018": "GROSSREGION_ID",
                "Arbeitsmarktregionen 2018": "REGION_ID",
            }
        )
        .astype({"GDENR": "int64", "GROSSREGION_ID": "int64", "REGION_ID": "int64"})
    )
    # replace IDs by names for REGION and GROSSREGION
    region_info = (
        region_info.assign(
            REGION=reg_names.loc[region_info["REGION_ID"]].values,
            GROSSREGION=grossreg_names.loc[region_info["GROSSREGION_ID"]].values,
        )
        .drop(["GROSSREGION_ID", "REGION_ID"], axis="columns")
        .set_index("GDENR")
    )

    return plz_unique, name_unique, plz_name_unique, region_info


########################################################################################
def match_regions(df_cust, df_plz_names, df_plz, df_names, df_regions):
    cust_data = df_cust.copy()
    cust_data.rename(columns={"EK_Plz": "PLZ", "EK_Ort": "GEMEINDE"}, inplace=True)

    def first_words(s):
        s = s.fillna("")
        return s.str.partition(expand=False).apply(lambda x: x[0])

    # Make PLZ numeric, replace (non-Swiss) non-numerics by -1
    cust_data["PLZ"] = (
        pd.to_numeric(cust_data["PLZ"], errors="coerce").fillna(-1).astype("int64")
    )
    # remap special names to the closest known town
    cust_data["GEMEINDE"] = cust_data["GEMEINDE"].replace(
        {
            "Schönbühl Einkaufszentrum": "Urtenen-Schönbühl",
            "Emmenbrücke 1": "Emmen",
            "Glattzentrum b. Wallisellen": "Wallisellen",
            "Zürich-Flughafen": "Kloten",
            "Büsingen": "Schaffhausen",
            "Serfontana": "Chiasso",
            "Triesen": "Sevelen",
            "Campione d'Italia": "Bissone",
            "Ponte Cremenaga": "Monteggio",
        }
    )
    # Disambiguate problem cases: Gossau, Buchs, Wil
    cust_data["GEMEINDE"] = cust_data["GEMEINDE"].replace(
        {
            "Gossau SG": "Gossau (SG)",
            "Gossau ZH": "Gossau (ZH)",
            #
            "Buchs AG": "Buchs (AG)",
            "Buchs SG": "Buchs (SG)",
            "Buchs SG 1": "Buchs (SG)",
            "Buchs ZH": "Buchs (ZH)",
            "Buchs LU": "Dagmersellen",
            #
            "Wil SG": "Wil (SG)",
            "Wil SG 1": "Wil (SG)",
            "Wil": "Wil (SG)",
            "Wil AG": "Mettauertal",
        }
    )
    # prepare a column with the first word of the NAME
    cust_data["FIRST_NAME"] = first_words(cust_data["GEMEINDE"])

    swiss = cust_data.EK_Land == "SCHWEIZ"

    # Find a GDENR for each Swiss customer:
    #
    # First, match by PLZ and town name
    cust_data.loc[swiss, "GDENR"] = (
        cust_data.loc[swiss, ["PLZ", "GEMEINDE"]]
        .merge(df_plz_names, on=["PLZ", "GEMEINDE"], how="left")["GDENR"]
        .values
    )

    # Try to fill missing by matching on PLZ
    missing = cust_data.GDENR.isnull() & swiss
    cust_data.loc[missing, "GDENR"] = (
        cust_data.loc[missing, ["PLZ"]]
        .merge(df_plz, on="PLZ", how="left")["GDENR"]
        .values
    )

    # Then, match by town name
    missing = cust_data.GDENR.isnull() & swiss
    cust_data.loc[missing, "GDENR"] = (
        cust_data.loc[missing, ["GEMEINDE"]]
        .merge(df_names, on="GEMEINDE", how="left")["GDENR"]
        .values
    )

    # Then, match by first word of town name only
    missing = cust_data.GDENR.isnull() & swiss
    cust_data.loc[missing, "GDENR"] = (
        cust_data.loc[missing, ["FIRST_NAME"]]
        .merge(
            pd.DataFrame(
                {
                    "FIRST_NAME": first_words(df_names["GEMEINDE"]),
                    "GDENR": df_names["GDENR"],
                }
            ).drop_duplicates(),
            on="FIRST_NAME",
            how="left",
        )["GDENR"]
        .values
    )

    # Check that we matched every swiss (PLZ, Ort) to a GDENR
    not_matched = cust_data.loc[cust_data.GDENR.isnull() & swiss, ["PLZ", "GEMEINDE"]]
    not_matched_count = not_matched.shape[0]
    if not_matched_count > 0:
        err(
            f"{not_matched_count} instances of unmatched (PLZ, Ort): \n{not_matched.drop_duplicates()}"
        )
        raise AssertionError(f"{not_matched_count} unmatched records")

    cust_data = cust_data.merge(df_regions, on="GDENR", how="left").drop(
        columns=["GDENR", "FIRST_NAME"]
    )
    return cust_data

########################################################################################
## VERKAUFGEBIETE

########################################################################################
def plz_data_olap_counter(column):
    agg_counter = (plz_data.groupby(column)
                        .agg({"VERKAUFS_GEBIETS_CODE": "count"})
                        .reset_index()
                        .rename(columns={"VERKAUFS_GEBIETS_CODE": "CNT"})) # COUNT
    return agg_counter

########################################################################################
def endkunde2vkgeb():
    ## Our basis table with our customers
    col_list = """Endkunde_NR
                  PLZ
                  GEMEINDE
                  KANTON
                  EK_Land""".split()

    cust_current = ek_info.loc[:,col_list]

    ## International customers get mapped to MAT, INTERNATIONAL

    # find the internationl customers
    row_select = (cust_current.loc[:,"EK_Land"] != "SCHWEIZ")
    cust_matched_international = cust_current.loc[row_select,:]

    # map international customers to (MAT, INTERNATIONAL)
    cust_matched_international.loc[:,"VERKAUFS_GEBIETS_CODE"] = "INTERNATIONAL"
    cust_matched_international.loc[:,"VB_VKGEB"] = "MAT"

    # matched, cleanup
    col_list     = """Endkunde_NR VERKAUFS_GEBIETS_CODE VB_VKGEB""".split()
    cust_matched = cust_matched_international.loc[:,col_list].copy()

    # unmatched, cleanup
    row_select = (pd.merge(cust_current,
                          cust_matched_international,
                          on="Endkunde_NR",
                          how="left")
                  .loc[:,"VERKAUFS_GEBIETS_CODE"]
                  .isna()
                 )
    cust_unmatched = cust_current.loc[row_select,:]

    #print("INTERNATIONAL")
    #report_success()

    ## Taking care of all the Swiss Customers:

    fraktion_cnt = plz_data_olap_counter("FRAKTION")
    plz_cnt      = plz_data_olap_counter("PLZ")
    ort_cnt      = plz_data_olap_counter("ORT")

    max_iters = max(fraktion_cnt.loc[:,"CNT"]
                    .append(plz_cnt.loc[:,"CNT"])
                    .append(ort_cnt.loc[:,"CNT"])
                   )+1

    for unique_by in range(1,max_iters):

        ## Match by FRAKTION
        plz_unique_fraktion = (pd.merge(plz_data,
                                        fraktion_cnt,
                                        left_on="FRAKTION",
                                        right_on="FRAKTION",
                                        how="left"
                                       )
                                 .query(f"CNT =={unique_by}") # only unique ones
                                 .groupby("FRAKTION")
                                 .agg(
                                     {"VERKAUFS_GEBIETS_CODE": collect,
                                      "VB_VKGEB": collect,
                                     }
                                 )
                                 .reset_index()
                              )

        cust_container_fraktion = (pd.merge(cust_unmatched,
                                          plz_unique_fraktion.loc[:,"""FRAKTION VERKAUFS_GEBIETS_CODE  VB_VKGEB""".split()],
                                          left_on="GEMEINDE",
                                          right_on="FRAKTION",
                                          how="left"
                                         )
                                 )

        row_select = cust_container_fraktion.loc[:,"VERKAUFS_GEBIETS_CODE"].isna() # unmatched rows!
        cust_matched_fraktion = cust_container_fraktion.loc[~row_select,"""Endkunde_NR VERKAUFS_GEBIETS_CODE VB_VKGEB""".split()]

        cust_matched          = cust_matched.append(cust_matched_fraktion).drop_duplicates()
        cust_unmatched        = cust_container_fraktion.loc[ row_select,"""Endkunde_NR PLZ GEMEINDE KANTON EK_Land""".split()]

        ## Match by PLZ
        plz_unique_plz = (pd.merge(plz_data,
                                        plz_cnt,
                                        left_on="PLZ",
                                        right_on="PLZ",
                                        how="left"
                                       )
                                 .query(f"CNT == {unique_by}") # only unique ones
                                 .groupby("PLZ")
                                 .agg(
                                     {"VERKAUFS_GEBIETS_CODE": collect,
                                      "VB_VKGEB": collect,
                                     }
                                 )
                                 .reset_index()
                              )

        cust_container_plz = (pd.merge(cust_unmatched,
                                       plz_unique_plz.loc[:,"""PLZ VERKAUFS_GEBIETS_CODE  VB_VKGEB""".split()],
                                       left_on="PLZ",
                                       right_on="PLZ",
                                       how="left"
                                      )
                             )

        row_select = cust_container_plz.loc[:,"VERKAUFS_GEBIETS_CODE"].isna() # unmatched rows!
        cust_matched_plz = cust_container_plz.loc[~row_select,"""Endkunde_NR VERKAUFS_GEBIETS_CODE VB_VKGEB""".split()]

        cust_matched     = cust_matched.append(cust_matched_plz).drop_duplicates()
        cust_unmatched   = cust_container_plz.loc[ row_select,"""Endkunde_NR PLZ GEMEINDE KANTON EK_Land""".split()]

        ## Match by ORT
        plz_unique_ort = (pd.merge(plz_data,
                                        ort_cnt,
                                        left_on="ORT",
                                        right_on="ORT",
                                        how="left"
                                       )
                                 .query(f"CNT == {unique_by}") # only unique ones
                                 .groupby("ORT")
                                 .agg(
                                     {"VERKAUFS_GEBIETS_CODE": collect,
                                      "VB_VKGEB": collect,
                                     }
                                 )
                                 .reset_index()
                              )

        cust_container_ort = (pd.merge(cust_unmatched,
                                       plz_unique_ort.loc[:,"""ORT VERKAUFS_GEBIETS_CODE  VB_VKGEB""".split()],
                                       left_on="GEMEINDE",
                                       right_on="ORT",
                                       how="left"
                                      )
                             )

        row_select = cust_container_ort.loc[:,"VERKAUFS_GEBIETS_CODE"].isna()
        cust_matched_ort = cust_container_ort.loc[~row_select,"""Endkunde_NR VERKAUFS_GEBIETS_CODE VB_VKGEB""".split()]

        cust_matched     = cust_matched.append(cust_matched_plz).drop_duplicates()
        cust_unmatched   = cust_container_ort.loc[ row_select,"""Endkunde_NR PLZ GEMEINDE KANTON EK_Land""".split()]
        
        # End of one iteration

        
    info(f"Verkaufsgebiete Matched: {cust_matched.shape[0]/cust_current.shape[0]}")
    return cust_matched

########################################################################################
def crm_info():
    today = pd.Timestamp.now()

    ## Information about last contact: Kanal, VB, Betreff
    row_select = (crm_data.loc[:,"STARTTERMIN"] <= today)
    container_crm = (crm_data.loc[row_select,:]
                             .sort_values(["ENDKUNDE_NR","STARTTERMIN"])
                             .groupby("ENDKUNDE_NR", as_index=False)
                             .agg(
                                 {"KUERZEL": last_notna,
                                  "KANAL": last_notna,
                                  "BETREFF": last_notna,
                                  #"STARTTERMIN": last_notna,
                                 }
                             )
                             .rename(columns={
                                 "KUERZEL": "Letzter_Kontakt",
                                 "KANAL": "Kanal",
                                 "BETREFF": "Betreff",
                                 "ENDKUNDE_NR": "Endkunde_NR",
                             })
                    )

    # Letzte CRM-Kontakte (alle der letzten zwei Kalendarjahre)
    row_select = (crm_data.loc[:,"STARTTERMIN"]
                          .apply(lambda x: 2 >= today.isocalendar()[0] - x.isocalendar()[0])
                  & (crm_data.loc[:,"STARTTERMIN"] <= today)
                 )

    crm_letzte_vbs = (crm_data.loc[row_select,:]
                              .groupby("ENDKUNDE_NR", as_index=False)
                              .agg(
                                  {"KUERZEL": collect,
                                  }
                              )
                              .rename(columns={
                                  "KUERZEL": "Letzte_CRM_Ktkts",
                                  "ENDKUNDE_NR": "Endkunde_NR",})
                     )
    # Combine
    container_crm = pd.merge(container_crm, crm_letzte_vbs, on="Endkunde_NR",how="left")

    return container_crm

########################################################################################
def booking_nettos_vbs(booking_raw):
    today = pd.Timestamp.now()
    booking_raw = booking_raw.astype({"Kamp_Erfass_Jahr": "int64"})

    ##
    row_select = ((booking_raw.loc[:,"Kampagnen_Status"] != 3) & # nur nicht-annulierte Kampagnen
                  (booking_raw.loc[:,"Kamp_Erfass_Jahr"] <= today.isocalendar()[0]) &
                  (booking_raw.loc[:,"Kamp_Erfass_Jahr"] >= (today.isocalendar()[0])-4) 
                 )

    booking_nettos = pd.pivot_table(
        booking_raw.loc[row_select,:],
        values  = ["Netto"],
        aggfunc = {"Netto": np.nansum},
        columns = ["Kamp_Erfass_Jahr"],
        index   = ["Endkunde_NR"],
        fill_value= 0
        )

    booking_nettos_flattened = pd.DataFrame(booking_nettos.to_records(index=False))
    booking_nettos_flattened.loc[:,"Endkunde_NR"] = pd.Series(booking_nettos.index)
    col_names = ["Net_"+str(x) for x in range(today.isocalendar()[0]-4, today.isocalendar()[0]+1)] + ["Endkunde_NR"]
    booking_nettos_flattened.columns = col_names

    ##
    row_select = ((booking_raw.loc[:,"Kampagnen_Status"] != 3) & # nur nicht-annulierte Kampagnen
                  (booking_raw.loc[:,"Kamp_Erfass_Jahr"] <= today.isocalendar()[0]) &
                  (booking_raw.loc[:,"Kamp_Erfass_Jahr"] >= (today.isocalendar()[0])-2) 
                 )

    booking_letzte_vbs = (booking_raw
        .loc[row_select,:]
        .groupby("Endkunde_NR", as_index=False)
        .agg({"Verkaufsberater": collect})
        .rename({"Verkaufsberater": "Letzte_VBs"})
        )

    ## merge
    ek_booking = pd.merge(
        booking_nettos_flattened,
        booking_letzte_vbs,
        on="Endkunde_NR",
        how="outer")
    
    return ek_booking

########################################################################################
# MAIN CODE
########################################################################################
info("Load booking data")
bd = load_bookings()
bd_copy = copy.deepcopy(bd)

info("Aggregate per customer")
customer_bd = aggregate_per_customer(bookings=bd)

info("Get region data")
(plz, names, plz_names, regions) = load_region_data()
ek_info = match_regions(
    df_cust=customer_bd,
    df_plz_names=plz_names,
    df_plz=plz,
    df_names=names,
    df_regions=regions,
)

###
info("\nLoad PLZ data: Verkaufsgebiete")
plz_data = load_plz()

info("Get Verkaufsgebiete data")
cust_vkgeb = endkunde2vkgeb()
ek_info = pd.merge(ek_info,
                   cust_vkgeb,
                   on="Endkunde_NR",
                   how="left"
                  )
###
info("\nLoad CRM data")
crm_data = load_crm()

info("Get CRM data, aggregation")
cust_crm = crm_info()

ek_info = pd.merge(ek_info,
                   cust_crm,
                   on="Endkunde_NR",
                   how="left"
                  )

###
info("Get Netto Sum & last succesful VBs")
ek_nettos_vbs = booking_nettos_vbs(booking_raw=bd_copy)

ek_info = pd.merge(ek_info,
                   ek_nettos_vbs,
                   on="Endkunde_NR",
                   how="left"
                  )


###
info("Write out result")
info(f"ek_info.shape: {ek_info.shape}")

with project_dir("vkprog"):
    store_bin(ek_info, "ek_info.feather")
