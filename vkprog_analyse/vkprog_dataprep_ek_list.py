#!/usr/bin/env python
# coding: utf-8
# Master File

########################################################################################
# # Tools & Libraries
########################################################################################

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
from pa_lib.file import load_bin

########################################################################################

def add_ek_info(scored_dataframe):
    ek_info = load_bin("vkprog/ek_info.feather")
    ek_list_raw = pd.merge(scored_dataframe, ek_info, on="Endkunde_NR", how="left")

    net_columns = [col for col in ek_info.columns if col.startswith("Net_")]

    col_row_filter = [
        "Insolvenz",
        # "Last_Res_Date",  # covered in listing
        # "Last_Aus_Date",  # covered in listing
        "last_CRM_Ktkt_date",
        "VB_FILTER_VON",
        "VB_FILTER_BIS",
    ]

    relevant_cols_deploy = (
        [
            "Endkunde_NR",  # Endkunde_NR
            "Endkunde",  # Endkunde
            "EK_HB_Apg_Kurzz",  # HB_APG (based on R-script)
            "Agentur",  # Agentur
            "AG_Hauptbetreuer",  # HB_Agentur
            "PLZ",  # PLZ
            "GEMEINDE",
        ]  # Ort
        + net_columns  # Net_2015, Net_2016, Net_2017, Net_2018, Net_2019
        + [
            "letzte_VBs",  # (bd, aggregiert)
            "Datum_Letzter_Ktkt",  # letzter Ktkt (Datum, crm)
            "Letzter_Kontakt",  # KZ_letzter_Ktkt (crm)
            "Kanal",  # (crm)
            "Betreff",  # (crm)
            "Last_Res_Date",  # Letzte_Kamp_erfasst
            "Last_Aus_Date",  # letzte_Kamp_Beginn
            "VERKAUFS_GEBIETS_CODE",  # Verkaufsgebiet
            "VB_VKGEB",
            "Prob_1",  # prob_KW (from here, good good.)
        ]
        # Needed for row_filter
        + col_row_filter
    )

    ek_list = ek_list_raw.loc[:, relevant_cols_deploy].rename(
        columns={
            "EK_HB_Apg_Kurzz": "HB_APG",
            "AG_Hauptbetreuer": "HB_Agentur",
            "GEMEINDE": "Ort",
            "Letzter_Kontakt": "letzter_Kontakt",
            "Last_Res_Date": "letzte_Kamp_erfasst",
            "Last_Aus_Date": "letzte_Kamp_Beginn",
            "VERKAUFS_GEBIETS_CODE": "Verkaufsgebiet",
            "VB_VKGEB": "VB_VK_Geb",
            "Prob_1": "prob_KW",
        }
    )
    return ek_list

########################################################################################
