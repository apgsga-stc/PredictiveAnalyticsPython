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

from pa_lib.file import store_bin, project_dir, data_files, load_csv, load_xlsx
from pa_lib.data import as_dtype, dtFactor, lookup, cut_categorical, merge_categories
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


def convert_ax_data(data, var_labels):
    # Add columns: logValue, Variable description
    result = data.assign(
        logValue=np.log(data["Value"]),
        VarDesc=lookup(var_labels, target_col_name="Label", match_col=data["Variable"]),
    ).pipe(as_dtype, to_dtype=dtFactor, incl_dtype="object")

    # Order weekdays correctly
    result["DayOfWeek"] = (
        result["DayOfWeek"]
        .astype("category")
        .cat.reorder_categories(
            "Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split(),
            ordered=True,
        )
    )

    # Add column: Time Slots
    result["TimeSlot"] = (
        result["Time"]
        .pipe(
            cut_categorical,
            left_limits="00:00 06:15 08:45 11:30 12:30 15:30 18:30 23:00".split(),
            labels="Nacht Morgen-Rush Morgen Mittag Nachmittag Abend-Rush Abend Nacht_spät".split(),
            cat_convert=lambda x: x[:5],  # match start time of interval in 'Time'
        )
        .pipe(merge_categories, cat="Nacht_spät", into_cat="Nacht")
    )

    result = result.sort_values(
        by=["Station", "DayOfWeek", "Time", "Variable", "Code"]
    ).reset_index(drop=True)
    return result


########################################################################################
# MAIN CODE
########################################################################################
ax_data, ax_var_labels = load_ax_data(source_dir="axinova_20190924")

with time_log("converting data"):
    ax_data = convert_ax_data(ax_data, ax_var_labels)

with project_dir("axinova"):
    store_bin(ax_data, "ax_data.feather")
    store_bin(ax_var_labels, "ax_var_labels.feather")
