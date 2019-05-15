#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 14:22:50 2019
Reads CRM data since 2009 from CRM prod
Query runtime: 2 min
@author: kpf
"""
import pandas as pd

from pa_lib.ora import Connection
from pa_lib.log import info, time_log
from pa_lib.file import store_csv, store_bin
from pa_lib.data import as_dtype, split_date_iso, make_isoweek_rd
from pa_lib.util import obj_size
from pa_lib.sql import query
from pa_lib.types import dtFactor

crm_query = query('crm')

info('Starting CRM query on CRM Prod instance')
with Connection('CRM_PROD') as c:
    crm_data_raw = c.long_query(crm_query)
info(f'Loaded data: {crm_data_raw.shape}, size is {obj_size(crm_data_raw)}')

info('Starting cleaning data')
with time_log('cleaning data'):
    crm_data = (crm_data_raw
        # filter
        .dropna(how='any',
                subset=('ENDKUNDE_NR', 'KANAL', 'QUELLE',
                        'STARTTERMIN', 'VERANTWORTLICH'))
        .drop_duplicates()
        # finish
        .sort_values(by=['ENDKUNDE_NR', 'STARTTERMIN'])
        .pipe(as_dtype, to_dtype=dtFactor, incl_dtype='object')
        .reset_index(drop=True)
    )

info(f'Cleaned data: {crm_data.shape}, size is {obj_size(crm_data)}')

# Write out data to CSV (runtime 14 sec)
info('Writing CRM data to data directory')
store_csv(crm_data, 'crm_data.csv', do_zip=True)
store_bin(crm_data, 'crm_data.feather')

info('Model-specific Data Management: Verkaufsprognose (VKProg)')
end_date = pd.Timestamp.today().normalize()
start_date = end_date.replace(year=end_date.year - 4)
crm_data_vkprog = (crm_data
    .rename({'STARTTERMIN': 'DATUM'}, axis='columns')
    # filter
    .query('@start_date <= DATUM')
    # enrich
    .pipe(split_date_iso, dt_col='DATUM', yr_col='YEAR', kw_col='KW')
    .pipe(make_isoweek_rd, kw_col='KW', round_by=(2, 4))
    .reset_index(drop=True)
)
store_bin(crm_data_vkprog, 'crm_data_vkprog.feather')

del (crm_data_raw, crm_data, crm_data_vkprog)
