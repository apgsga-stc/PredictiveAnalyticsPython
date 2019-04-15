#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 13:02:25 2019
Central constants for PA code
@author: kpf
"""

import sys

from pa_lib.types import ConnectPar, Record

# Directories
PA_LOG_DIR  = '/home/pa/logs/'
PA_DATA_DIR = '/home/pa/data/'

# Database connections
PA_ORA_DSN_TEMPL = '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={}.apgsga.ch)(PORT=1521)))(CONNECT_DATA=(SID={})))'

PA_ORA_CONN = Record(
    IT21_PROD   = ConnectPar('chpi211', 'it21',    'it21nurzumlesen12ti'),
    CRM_PROD    = ConnectPar('chpcrm1', 'ors_apg', 'ors_pwd_apg'),
    IT21_DEV_VK = ConnectPar('chei211', 'vk',      'vk_pass')
)


if __name__ == "__main__":
    test = Record(a=1, b=2, c='abc', d=[1,2,3], e=Record(e1='a', e2='b'))
    print(test)
    print(PA_ORA_CONN)