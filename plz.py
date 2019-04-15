#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 16:24:38 2019
Reads PLZ mapping from IT21 prod
Query runtime: 3.5 sec
@author: kpf
"""

from pa_lib.ora   import Connection
from pa_lib.log   import info
from pa_lib.data  import store_csv, store_bin, as_dtype
from pa_lib.util  import obj_size
from pa_lib.sql   import QUERY
from pa_lib.types import dtFactor

plz_query = QUERY['plz']

info('Starting PLZ query on IT21 Prod instance')
with Connection('IT21_PROD') as c:
    plz_data_raw = c.long_query(plz_query)
info(f'Finished PLZ query, returned {obj_size(plz_data_raw)} of data: {plz_data_raw.shape}')

# Write out raw data to CSV (runtime 0.02 sec)
info('Writing PLZ data to data directory')
store_csv(plz_data_raw, 'plz_data.csv', zip=True)

info('Starting cleaning data')
plz_data = (plz_data_raw
    .sort_values(by=['PLZ', 'FRAKTION', 'ORT'])
    .pipe(as_dtype, to_dtype=dtFactor, incl_col=('VERKAUFS_GEBIETS_CODE', 'VB_VKGEB'))
    .pipe(as_dtype, to_dtype='uint16', incl_col='PLZ')
    .reset_index(drop=True)
)
info(f'Cleaned data: {plz_data.shape}, size is {obj_size(plz_data)}')

info('Writing cleaned PLZ data to data directory')
store_bin(plz_data, 'plz_data.feather')

del(plz_data_raw, plz_data)