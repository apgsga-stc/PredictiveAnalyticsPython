# Load Axinova Data files from the APG intranet
# Will only work when run under Windows

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
import numpy as np

from pa_lib.types import Record
from pa_lib.file import store_bin, project_dir, data_files, load_csv
from pa_lib.data import as_dtype, dtFactor
from pa_lib.log import time_log


########################################################################################
def load_ax_data(source_dir):
    # how to find & read our data files
    data_pattern = "*Bahnhof_Uhrzeit_*.csv"
    data_params = dict(encoding="cp1252")
    label_pattern = "*VariablenBeschreibung.xlsx"

    data_by_month = {}
    with project_dir(source_dir):
        # read each data file
        file_list = data_files(data_pattern)
        if file_list.shape[0] == 0:
            raise FileNotFoundError(
                f"No data file matches '{data_pattern}' in data subdirectory '{source_dir}'"
            )
        for data_file in file_list.index:
            # file is like '190016Bahnhof_Uhrzeit_201905.csv'
            yearmonth = data_file.rsplit(".", maxsplit=1)[0][-6:]
            data_by_month[yearmonth] = load_csv(data_file, **data_params)

        # read label file
        file_list = data_files(label_pattern)
        if file_list.shape[0] == 0:
            raise FileNotFoundError(
                f"No label file matches '{data_pattern}' in data subdirectory '{source_dir}'"
            )
        elif file_list.shape[0] > 1:
            raise Exception(
                f"Multiple label files match '{data_pattern}' in data subdirectory '{source_dir}'"
            )
        label_file = file_list.index[0]
        var_labels = load_xlsx(
            label_file, sheet_name="Variablenbeschreibung", usecols=[0, 1, 2], header=0
        )

    with time_log("merging data"):
        all_data = pd.DataFrame(
            columns=data_by_month[list(data_by_month.keys())[0]].columns
        )
        for yearmonth in data_by_month.keys():
            all_data = all_data.append(
                data_by_month[yearmonth].assign(
                    Year=yearmonth[:4], Month=yearmonth[-2:]
                ),
                sort=False,
                ignore_index=True,
            )

    return all_data, var_labels


########################################################################################
# MAIN CODE
########################################################################################
ax_data, ax_var_labels = load_ax_data(source_dir="axinova_20190906")
with time_log("converting data"):
    ax_data = ax_data.assign(
        logValue=np.log(ax_data['Value']),
        Station_Time=ax_data['Station'].str.cat(ax_data['Time'], sep=" "),
        VarDesc=(ax_data.merge(ax_var_labels, how="left", on="Variable")["Label"]).values,
    ).pipe(as_dtype, to_dtype=dtFactor, incl_dtype="object")
    ax_data.DayOfWeek = ax_data.DayOfWeek.astype("category").cat.reorder_categories(
        "Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split(), ordered=True
    )
    ax_data = ax_data.sort_values(
        by=["Station", "DayOfWeek", "Variable", "Code"]
    ).reset_index(drop=True)

with project_dir("axinova"):
    store_bin(ax_data, "ax_data.feather")
    store_bin(ax_var_labels, "ax_var_labels.feather")
