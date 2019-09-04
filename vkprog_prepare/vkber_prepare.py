#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Read all VB (Verkaufsberater) that are currently active
"""
# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

from pa_lib.ora  import Connection
from pa_lib.log  import info
from pa_lib.file import store_csv, store_bin, write_xlsx, set_project_dir
from pa_lib.sql  import query

set_project_dir('vkprog')
vkber_query = query('vkber')

info('Starting Verkaufsberater query on IT21 Prod instance')
with Connection('IT21_PROD') as c:
    vkber_data = c.query(vkber_query)
info(f'Finished Verkaufsberater query, returned data: {vkber_data.shape}')

# Write out to files
info('Writing Verkaufsberater Daten to data directory')

write_xlsx(vkber_data, 'vkber_data.xlsx', sheet_name='Verkaufsberater')
store_csv(vkber_data, 'vkber_data.csv', do_zip=False)
store_bin(vkber_data, 'vkber_data.feather')