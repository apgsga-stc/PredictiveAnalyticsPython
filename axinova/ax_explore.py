import pandas as pd
import csv

from pa_lib.file import load_csv
from pa_lib.data import as_dtype, desc_col
from pa_lib.type import dtFactor

ax_raw = load_csv('axinova_20190606/190016Wochentage.csv', sep=';', quoting=csv.QUOTE_NONE, encoding='cp1252')

ax = (ax_raw
      .pipe(as_dtype, dtFactor, incl_dtype='object'))

