#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 14:29:51 2019
Reads Buchungen data since 2009 from IT21 prod
Query runtime: 11 min
@author: pa
"""
import pandas as pd
import numpy as np

from pa_lib.ora import Connection
from pa_lib.log import info
from pa_lib.file import store_csv, store_bin
from pa_lib.df import (as_dtype, clean_up_categoricals, replace_col,
                       calc_col_partitioned, split_date_iso, make_isoweek_rd)
from pa_lib.util import obj_size, last_monday
from pa_lib.sql import QUERY
from pa_lib.types import dtFactor

bd_query = QUERY['bd']

info('Starting Buchungsdaten query on IT21 Prod instance')
with Connection('IT21_PROD') as c:
    bd_data_raw = c.long_query(bd_query)
info(f'Finished Buchungsdaten query, returned data: {bd_data_raw.shape}')

info(f'Convert data types {bd_data_raw.shape}')
bd_data_raw = (bd_data_raw
    .pipe(as_dtype, to_dtype=dtFactor, incl_dtype='object',
          incl_col=('ENDKUNDE_NR', 'EK_AKTIV', 'KAMPAGNEN_STATUS'))
)

# Write out to CSV (runtime 2 min)
info('Writing raw Buchungsdaten to data directory')
store_csv(bd_data_raw, 'bd_data.csv', do_zip=True)
store_bin(bd_data_raw, 'bd_data_raw.feather')

################################################################################
col_list = """ENDKUNDE_NR
              ENDKUNDE
              EK_ABC
              EK_PLZ
              EK_ORT
              EK_LAND
              EK_HB_APG_KURZZ
              EK_AKTIV
              AGENTUR
              VERKAUFSBERATER
              ENDKUNDE_BRANCHENGRUPPE_ID
              ENDKUNDE_BRANCHENGRUPPE
              ENDKUNDE_NBRANCHENGRUPPE_ID
              ENDKUNDE_NBRANCHENGRUPPE
              ENDKUNDE_BRANCHENKAT_ID
              ENDKUNDE_BRANCHENKAT
              ENDKUNDE_NBRANCHENKAT_ID
              ENDKUNDE_NBRANCHENKAT
              AUFTRAG_BRANCHENGRUPPE_ID
              AUFTRAG_BRANCHENGRUPPE
              AUFTRAG_NBRANCHENGRUPPE_ID
              AUFTRAG_NBRANCHENGRUPPE
              AUFTRAG_BRANCHENKAT_ID
              AUFTRAG_BRANCHENKAT
              AUFTRAG_NBRANCHENKAT_ID
              AUFTRAG_NBRANCHENKAT
              AGPS_NR
              SEGMENT
              KV_NR 
              KV_TYP
              KAMPAGNEN_STATUS
              KAMPAGNE_ERFASSUNGSDATUM
              KAMPAGNE_BEGINN
              AUFTRAGSART
              RES_DAT
              ANNULLATION_DATUM
              AUSH_VON
              DAUER
              VERTRAG
              BRUTTO
              NETTO
              AGGLO
              PF
           """.split()

info(f'Starting cleaning data {bd_data_raw.shape}')

info('Filter')
bd_data = (bd_data_raw
    # filter
    .loc[:, col_list]
    .dropna(how='any',
           subset="""ENDKUNDE_NR ENDKUNDE EK_AKTIV VERKAUFSBERATER AGPS_NR SEGMENT KV_NR 
              KAMPAGNEN_STATUS KAMPAGNE_ERFASSUNGSDATUM AUFTRAGSART RES_DAT AUSH_VON
              DAUER VERTRAG BRUTTO NETTO""".split())
    .query('not KAMPAGNE_BEGINN < KAMPAGNE_ERFASSUNGSDATUM')  # keep rows where KAMPAGNE_BEGINN is empty!
    .query('DAUER >= 0')
)

info('Data Corrections')
bd_data = (bd_data
    # fix ANNULLATION_DATUM < RES_DAT
    .pipe(replace_col, 'ANNULLATION_DATUM', with_col='RES_DAT',
          where='ANNULLATION_DATUM < RES_DAT')
    # fix KAMPAGNE_ERFASSUNGSDATUM < min(RES_DAT) per KV
    .pipe(calc_col_partitioned, 'KV_RES_DAT', fun=np.min, on='RES_DAT',
          part_by='KV_NR')
    .pipe(replace_col, 'KAMPAGNE_ERFASSUNGSDATUM', with_col='KV_RES_DAT',
          where='KAMPAGNE_ERFASSUNGSDATUM < KV_RES_DAT')
    .drop('KV_RES_DAT', axis='columns')
    .pipe(clean_up_categoricals)
    .reset_index(drop=True)
)

info(f'Cleaned data: {bd_data.shape}, size is {obj_size(bd_data)}')
store_bin(bd_data, 'bd_data.feather')

################################################################################
info('Model-specific Data Management: Verkaufsprognose (VKProg)')
info('Data Filtering')
end_date = last_monday(pd.Timestamp.today())
start_date = end_date.replace(year=end_date.year - 4)

bd_data_vkprog = (bd_data
    .query('not @start_date >= KAMPAGNE_BEGINN and not KAMPAGNE_BEGINN > @end_date')  # keep empties!
    .query('DAUER < 62')
    .query('AUFTRAGSART != ["Eigenwerbung APG|SGA", "Aushangauftrag Partner", "Logistik für Dritte", "Politisch"]')
    .query('not SEGMENT == "Airport" and not KV_TYP == "KPGL"')
    .pipe(clean_up_categoricals)
    .reset_index(drop=True)
)

info('Data Enrichment')
bd_data_vkprog = (bd_data_vkprog
    .pipe(split_date_iso, 'KAMPAGNE_BEGINN',
          yr_col='KAMP_BEGINN_JAHR', kw_col='KAMP_BEGINN_KW')
    .pipe(make_isoweek_rd, kw_col='KAMP_BEGINN_KW', round_by=(2, 4))
    .pipe(split_date_iso, 'KAMPAGNE_ERFASSUNGSDATUM',
          yr_col='KAMP_ERFASS_JAHR', kw_col='KAMP_ERFASS_KW')
    .pipe(make_isoweek_rd, kw_col='KAMP_ERFASS_KW', round_by=(2, 4))
)

store_bin(bd_data_vkprog, 'bd_data_vkprog.feather')

del (bd_data_raw, bd_data, bd_data_vkprog)
