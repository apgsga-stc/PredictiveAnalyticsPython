# Get Dispo Opening Dates from Excel files on the APG intranet
# Will only work when run under Windows

# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd

from pa_lib.log import err, info
from pa_lib.file import store_bin, write_xlsx, project_dir
from pa_lib.data import split_date_iso, make_isoweek_rd


########################################################################################
def get_all_files():
    # Find all source files for dispo planning
    base_dir = Path(r"P:\Projekte\produktmanagement\buchunsgfenster")  # not a typo(!)
    if not base_dir.exists():
        raise FileNotFoundError(base_dir)

    files = {}
    for dispo in base_dir.glob("20??-?"):
        if (
            dispo.is_dir()
            and dispo.stem > "2006-1"  # file in directory '2006-1' has no date
        ):
            suffix = dispo.stem.replace("-", "?")
            file_pattern = f"Ablauf?Buchungsfenster*{suffix}.xls*"
            file_list = list(dispo.glob(file_pattern))
            if len(file_list) == 0:
                err(f"No file matching {file_pattern} found in directory {dispo}")
                continue
            elif len(file_list) > 1:
                err(
                    f"Several files matching {file_pattern} found in directory {dispo}: {file_list}"
                )
                continue
            else:
                files[dispo.stem] = file_list[0]

    return files


########################################################################################
def extract_dates(files):
    # Prepare data structure
    dates = pd.DataFrame.from_records(
        columns=["KAM_open_date", "open_date"],
        index=pd.CategoricalIndex(files.keys(), name="Dispo", ordered=True),
        data=[],
    ).astype({"KAM_open_date": "datetime64", "open_date": "datetime64"})

    # Read dates from files
    for (dispo, dispo_file) in files.items():
        first_sheet = pd.read_excel(
            dispo_file, header=None, nrows=2, parse_dates=[3, 5]
        )
        if dispo <= "2011-1":
            dispo_date = first_sheet.iat[1, 3]
        elif dispo >= "2011-2":
            dispo_date = first_sheet.iat[1, 5]
        else:
            continue
        dates.loc[dispo, "KAM_open_date"] = pd.to_datetime(
            dispo_date, format="%Y-%m-%d"
        )

    return dates


########################################################################################
def cleanup_dates(dates):
    # Normal opening dates are one week after KAM opening dates. Also clean up data types
    dates = (
        dates.dropna(how="all")
        .pipe(split_date_iso, "KAM_open_date", yr_col="Jahr", kw_col="KAM_KW")
        .pipe(make_isoweek_rd, kw_col="KAM_KW", round_by=2)
        .assign(open_date=dates.KAM_open_date + pd.offsets.Week())
        .pipe(split_date_iso, "open_date", yr_col="Jahr", kw_col="KW")
        .pipe(make_isoweek_rd, kw_col="KW", round_by=2)
        .drop(columns=["KAM_KW", "KW"])
    )
    return dates


########################################################################################
def aggregate_per_year(dates):
    # Bring together both openings for each year
    def kw_row(df, kw_columns):
        dispo_1 = (
            df.iloc[[0], kw_columns]
            .reset_index(drop=True)
            .set_axis(["KAM_1", "Alle_1"], axis="columns", inplace=False)
        )
        dispo_2 = (
            df.iloc[[1], kw_columns]
            .reset_index(drop=True)
            .set_axis(["KAM_2", "Alle_2"], axis="columns", inplace=False)
        )
        return pd.concat([dispo_1, dispo_2], axis="columns")

    periods = (
        dates.sort_values(["Jahr", "KAM_KW_2"])
        .groupby("Jahr")
        .apply(kw_row, kw_columns=list(dates.columns.get_indexer(["KAM_KW_2", "KW_2"])))
        .reset_index(level=1, drop=True)
    )
    return periods


########################################################################################
# MAIN CODE
########################################################################################
info("Read source files from intranet, clean up dates")
dispo_files = get_all_files()
dispo_dates = extract_dates(dispo_files).pipe(cleanup_dates)

info("Aggregate periods to years")
dispo_periods_yr = aggregate_per_year(dispo_dates)

info("Write dispo opening dates to project directory")
with project_dir("vkprog"):
    store_bin(dispo_dates, "dispo.feather")
    store_bin(dispo_periods_yr, "dispo_periods.feather")
    write_xlsx(dispo_dates, "dispo.xlsx", sheet_name="Dispo-Eröffnungen")
