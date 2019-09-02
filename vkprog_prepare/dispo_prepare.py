# Get Dispo Opening Dates from Excel files on the APG intranet
# Will only work when run under Windows

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd

from pa_lib.log import err
from pa_lib.file import store_bin, write_xlsx

# Find all source files for dispo planning
base_dir = Path(r'P:\Projekte\produktmanagement\buchunsgfenster')  # not a typo, that IS the directory's name
if not base_dir.exists():
    raise FileNotFoundError(base_dir)

dispo_files = {}
for dispo in base_dir.glob('20??-?'):
    if dispo.is_dir() and dispo.stem > '2006-1':  # file in '2006-1' has no date
        suffix = dispo.stem.replace('-', '?')
        file_pattern = f'Ablauf?Buchungsfenster*{suffix}.xls*'
        file_list = list(dispo.glob(file_pattern))
        if len(file_list) == 0:
            err(f'No file matching {file_pattern} found in directory {dispo}')
            continue
        elif len(file_list) > 1:
            err(f'Several files matching {file_pattern} found in directory {dispo}: {file_list}')
            continue
        else:
            dispo_files[dispo.stem] = file_list[0]

# Read dates from files
dispo_dates = pd.DataFrame.from_records(columns=['KAM_open_date', 'open_date'],
                                        index=pd.CategoricalIndex(dispo_files.keys(), name='Dispo'),
                                        data=[])
for dispo in dispo_files.keys():
    first_sheet = pd.read_excel(dispo_files[dispo], header=None, skiprows=[0], nrows=1,
                                parse_dates=[3, 5])
    if dispo <= '2011-1':
        dispo_date = first_sheet.iat[0, 3]
    elif '2011-2' <= dispo:
        dispo_date = first_sheet.iat[0, 5]
    else:
        continue
    dispo_dates.loc[dispo, 'KAM_open_date'] = pd.to_datetime(dispo_date, format='%Y-%m-%d %h24:%mi:%s')

# Normal opening dates are one week after KAM opening dates. Also clean up data types
dispo_dates = (dispo_dates
               .dropna(how='all')
               .astype({'KAM_open_date': 'datetime64'})
               .assign(open_date=dispo_dates.KAM_open_date + pd.offsets.Week())
               )

# Write out dates to data directory
store_bin(dispo_dates, 'dispo.feather')
write_xlsx(dispo_dates, 'dispo.xlsx', sheet_name='Dispo-Eröffnungen')



