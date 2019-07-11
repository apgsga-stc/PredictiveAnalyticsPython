#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Read all VB (Verkaufsberater) that are currently active
"""
from pa_lib.ora  import Connection
from pa_lib.log  import info
from pa_lib.file import store_csv, store_bin
from pa_lib.sql  import query

vkber_query = query('vkber')

info('Starting Verkaufsberater query on IT21 Prod instance')
with Connection('IT21_PROD') as c:
    vkber_data = c.query(vkber_query)
info(f'Finished Verkaufsberater query, returned data: {vkber_data.shape}')

# Write out to CSV
info('Writing Verkaufsberater Daten to data directory')
store_csv(vkber_data, 'vkber_data.csv', do_zip=False)
store_bin(vkber_data, 'vkber_data.feather')