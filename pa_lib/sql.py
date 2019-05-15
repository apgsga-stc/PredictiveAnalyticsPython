#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface to queries for PA data exports (in subdirectory 'sql', one file each)

@author: kpf
"""

from pathlib import Path
from pa_lib.file import file_list

# Dict QUERY holds all queries under their tag (file basename)
QUERY = {}

def init():
    sql_path = Path('pa_lib', 'sql').resolve()
    for sql_file in file_list(sql_path, '*.sql').name:
        tag = sql_file[0:-4]
        with open(sql_path / sql_file) as query_file:
            QUERY[tag] = query_file.read()
        
        
def query(tag):
    try:
        return QUERY[tag]
    except KeyError:
        init()
        return QUERY[tag]