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
def aggregate_per_customer(bookings):
    def last_notna(s):
        try:
            return s.loc[s.notnull()].iat[-1]
        except IndexError:
            return np.NaN

    def collect(s, sep=","):
        return sep.join(map(str, s[s.notna()].unique()))

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
            }
        )
    )

    customer_info.set_axis(
        labels="""Endkunde_NR Endkunde EK_Aktiv EK_Kam_Betreut EK_Land EK_Plz EK_Ort Agentur EK_BG 
                  EK_BG_ID Auftrag_BG_ID Auftrag_BG_Anz Last_Res_Date First_Res_Year 
                  Last_Res_Year Last_Aus_Date""".split(),
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
                "GDENAMK": "NAME",
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
        return df.loc[df.PRC.idxmax(), ["PLZ", "KANTON", "GDENR", "NAME"]]

    plz_unique = plz_raw.groupby("PLZ", as_index=False).apply(main_features)
    name_unique = plz_raw.groupby("NAME", as_index=False).apply(main_features)
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
    cust_data.rename(columns={"EK_Plz": "PLZ", "EK_Ort": "NAME"}, inplace=True)

    def first_words(s):
        s = s.fillna("")
        return s.str.partition(expand=False).apply(lambda x: x[0])

    # Make PLZ numeric, replace (non-Swiss) non-numerics by -1
    cust_data["PLZ"] = (
        pd.to_numeric(cust_data["PLZ"], errors="coerce").fillna(-1).astype("int64")
    )
    # remap special names to the closest known town
    cust_data["NAME"] = cust_data["NAME"].replace(
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
    cust_data["NAME"] = cust_data["NAME"].replace(
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
    cust_data["FIRST_NAME"] = first_words(cust_data["NAME"])

    swiss = cust_data.EK_Land == "SCHWEIZ"

    # Find a GDENR for each Swiss customer:
    #
    # First, match by PLZ and town name
    cust_data.loc[swiss, "GDENR"] = (
        cust_data.loc[swiss, ["PLZ", "NAME"]]
        .merge(df_plz_names, on=["PLZ", "NAME"], how="left")["GDENR"]
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
        cust_data.loc[missing, ["NAME"]]
        .merge(df_names, on="NAME", how="left")["GDENR"]
        .values
    )

    # Then, match by first word of town name only
    missing = cust_data.GDENR.isnull() & swiss
    cust_data.loc[missing, "GDENR"] = (
        cust_data.loc[missing, ["FIRST_NAME"]]
        .merge(
            pd.DataFrame(
                {
                    "FIRST_NAME": first_words(df_names["NAME"]),
                    "GDENR": df_names["GDENR"],
                }
            ).drop_duplicates(),
            on="FIRST_NAME",
            how="left",
        )["GDENR"]
        .values
    )

    # Check that we matched every swiss (PLZ, Ort) to a GDENR
    not_matched = cust_data.loc[cust_data.GDENR.isnull() & swiss, ["PLZ", "NAME"]]
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
# MAIN CODE
########################################################################################
info("Load booking data")
bd = load_bookings()

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

info("Write out result")
with project_dir("vkprog"):
    store_bin(ek_info, "ek_info.feather")
