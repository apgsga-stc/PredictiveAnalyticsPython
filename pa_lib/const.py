#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 13:02:25 2019
Central constants for PA code
@author: kpf
"""

import os
from pathlib import Path

from pa_lib.type import ConnectPar, Record


########################################################################################################################
# DIRECTORIES
########################################################################################################################
# base directory of the code environment
PA_BASE_DIR = os.getenv("PA_BASE_DIR", Path.home())
# where pa_lib.log will write
PA_LOG_DIR = os.getenv("PA_LOG_DIR", PA_BASE_DIR / "logs")
# base directory of all project (data) directories
PA_DATA_DIR = os.getenv("PA_DATA_DIR", PA_BASE_DIR / "data")
# where pa_lib.job looks for configuration
PA_JOB_DIR = os.getenv("PA_JOB_DIR", PA_BASE_DIR / "jobs")


########################################################################################################################
# DATABASE CONNECTIONS
########################################################################################################################
PA_ORA_DSN_TEMPL = (
    "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={}.apgsga.ch)(PORT=1521)))"
    + "(CONNECT_DATA=(SID={})))"
)

PA_ORA_CONN = Record(
    IT21_PROD=ConnectPar(instance="chpi211", user="it21", passwd="it21nurzumlesen12ti"),
    CRM_PROD=ConnectPar(instance="chpcrm1", user="ors_apg", passwd="ors_pwd_apg"),
    IT21_DEV_VK=ConnectPar(instance="chei211", user="vk", passwd="vk_pass"),
    APC_PROD_VDWH1=ConnectPar(instance="chpapc1", user="vdwh1", passwd="vdwh1_pass"),
)
