## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))


import pandas as pd
import numpy as np
from dataclasses import dataclass


from pa_lib.file import (
    project_dir,
    load_bin,
    load_pickle,
)
from pa_lib.data import (
    chi2_expected,
    clean_up_categoricals,
)
from pa_lib.util import (
    normalize_rows,
    as_percent,
    flat_list,
    list_items,
)
from pa_lib.log import warn

########################################################################################
# Load data sets
########################################################################################
with project_dir("axinova"):
    ax_data = load_bin("ax_data.feather")
    spr_data = load_pickle("spr_data.pkl")
    time_codes = load_pickle("time_code_ratios.pkl")
    station_codes = load_pickle("station_code_ratios.pkl")
    global_codes = load_pickle("global_code_ratios.pkl")
    population_codes = load_pickle("population_ratios.pkl")
    ax_var_struct = load_bin("ax_var_struct.feather")

all_stations = ax_data["Station"].cat.categories.to_list()
all_weekdays = ax_data["DayOfWeek"].cat.categories.to_list()


########################################################################################
# Select Axinova data by any column(s)
########################################################################################
def _check_selection(data, selection, allowed_columns):
    allowed_values = {}
    for column in allowed_columns:
        allowed_values[column] = data[column].cat.categories
    if set(selection.keys()) - set(allowed_columns) != set():
        raise NameError(f"Illegal column name in selection: {selection.keys()}")
    clean_selection = {}
    for column in allowed_columns:
        if column in selection:
            col_values = flat_list(selection[column])
            if set(col_values) - set(allowed_values[column]) != set():
                raise ValueError(
                    f"Illegal value(s) in parameter {column}: {col_values}"
                )
            clean_selection[column] = col_values
        else:
            clean_selection[column] = None
    return clean_selection


def select_data(all_data, **selection):
    """
    Filter ax_data by different columns. Supports sequences of allowed values.
    """
    select_columns = (
        "DayOfWeek Station Variable Month TimeSlot Hour "
        + "Time TimeSlot_cat StationSprache Code"
    ).split()
    selection = _check_selection(all_data, selection, allowed_columns=select_columns)
    row_mask = pd.Series([True] * all_data.shape[0])
    for col in select_columns:
        if selection[col] is not None:
            row_mask &= all_data[col].isin(selection[col])
    return all_data.loc[row_mask].pipe(clean_up_categoricals).reset_index(drop=True)


########################################################################################
# Look up code ratios from Axinova data
########################################################################################
RatioTable = pd.DataFrame


@dataclass
class Ratios:
    actual: RatioTable
    expected: RatioTable
    sd: RatioTable = None


def ax_population_ratios(variable: str, percent: bool = False) -> RatioTable:
    ratios = population_codes.query("Variable == @variable").pivot_table(
        values="Pop_Ratio", index="Variable", columns="Code"
    )
    return as_percent(ratios) if percent else ratios


def ax_global_ratios(variable: str, percent: bool = False) -> RatioTable:
    ratios = global_codes.query("Variable == @variable").pivot_table(
        values="Ratio", index="Variable", columns="Code"
    )
    return as_percent(ratios) if percent else ratios


def ax_station_ratios(variable: str, percent: bool = False) -> Ratios:
    actual_ratios = station_codes.query("Variable == @variable").pivot_table(
        values="Ratio", index="Station", columns="Code", fill_value=0
    )
    expected_ratios = ax_global_ratios(variable)
    if percent:
        result = Ratios(
            actual=as_percent(actual_ratios), expected=as_percent(expected_ratios)
        )
    else:
        result = Ratios(actual=actual_ratios, expected=expected_ratios)
    return result


def ax_ratios(
    variable: str,
    stations: str,
    weekdays: str,
    reference: str = "all_stations",
    time_scale: str = "Hour",
    percent: bool = False,
) -> Ratios:
    subset = ax_data.loc[
        ax_data.Station.isin(flat_list(stations))
        & ax_data.DayOfWeek.isin(flat_list(weekdays))
        & (ax_data.Variable == variable)
    ]
    full_index = [
        (weekday, time)
        for weekday in flat_list(weekdays)
        for time in ax_data[time_scale].cat.categories
    ]
    actual_counts = subset.pivot_table(
        values="Value",
        index=["DayOfWeek", time_scale],
        columns="Code",
        fill_value=0,
        aggfunc="sum",
    )
    actual_counts_sd_ratios = (
        (np.sqrt(actual_counts) / actual_counts)
        .fillna(0)
        .reindex(full_index, fill_value=0)
    )
    actual_ratios = normalize_rows(actual_counts).reindex(full_index, fill_value=0)

    if reference == "all_stations":
        expected_ratios = (
            time_codes[time_scale]
            .query("Variable == @variable")
            .pivot_table(
                values="Ratio",
                index=["DayOfWeek", time_scale],
                columns="Code",
                fill_value=0,
            )
        )
    elif reference == "station":
        expected_counts = chi2_expected(actual_counts)
        expected_ratios = normalize_rows(expected_counts).reindex(
            full_index, fill_value=0
        )
    else:
        raise ValueError(
            "Parameter 'reference' must be one of "
            + f"('station', 'all_stations'), was '{reference}'"
        )

    if percent:
        result = Ratios(
            actual=as_percent(actual_ratios),
            expected=as_percent(expected_ratios),
            sd=as_percent(actual_counts_sd_ratios),
        )
    else:
        result = Ratios(
            actual=actual_ratios, expected=expected_ratios, sd=actual_counts_sd_ratios
        )
    return result


########################################################################################
# Look up SPR+ data split by variable
########################################################################################
class NoSPRDataFound(Exception):
    """Raised when no SPR+ data was found for the given parameters"""

    pass


def spr_split(
    stations,
    variable,
    weekdays=None,
    reference="station",
    time_scale="Hour",
    type="abs",
    incl_totals=True,
    decimals=1,
):
    if type not in ["abs", "rel"]:
        raise ValueError(
            f"Parameter 'type' must be one of ('abs', 'rel'), was '{type}'"
        )
    if reference not in ["station", "all_stations"]:
        raise ValueError(
            "Parameter 'reference' must be one of "
            + f"('station', 'all_stations'), was '{reference}'"
        )
    if weekdays is None:
        weekdays = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]

    spr_counts = (
        spr_data.loc[
            spr_data.Station.isin(flat_list(stations))
            & spr_data.DayOfWeek.isin(weekdays)
        ]
        .groupby(["DayOfWeek", time_scale], observed=True)[["Total"]]
        .agg("sum")
    )
    if spr_counts.shape[0] == 0:
        raise NoSPRDataFound(dict(stations=stations, weekdays=weekdays))
    spr_count_sd_ratios = (np.sqrt(spr_counts) / spr_counts).fillna(0)

    ratios = ax_ratios(
        stations=stations,
        weekdays=weekdays,
        variable=variable,
        reference=reference,
        time_scale=time_scale,
    )
    if type == "abs":
        code_ratios = ratios.actual
    else:  # type == "rel"
        code_ratios = ratios.actual - ratios.expected
    split_counts = code_ratios.mul(spr_counts.Total, axis="index").round(decimals)

    sd_ratios = np.sqrt(
        (ratios.sd ** 2).add((spr_count_sd_ratios ** 2).values, axis="index")
    )

    if incl_totals:
        split_counts.set_axis(
            split_counts.columns.to_list(), axis="columns", inplace=True
        )
        split_counts["Total"] = spr_counts.round(decimals)
        sd_ratios.set_axis(sd_ratios.columns.to_list(), axis="columns", inplace=True)
        sd_ratios["Total"] = spr_count_sd_ratios.values
    if decimals == 0:
        split_counts = split_counts.astype("int")

    return split_counts, sd_ratios


########################################################################################
# Target audience ratios
########################################################################################
var_info = {}
for var, data in ax_var_struct.groupby("Variable"):
    var_info[var] = dict(
        Label=data["Variable_Label"].max(),
        Codes=data["Label"].to_list(),
        Order=list(range(len(data["Label_Nr"].to_list()))),
    )


def var_label(variable):
    return var_info[variable]["Label"]


def var_code_labels(variable):
    return var_info[variable]["Codes"]


def var_code_order(variable):
    return var_info[variable]["Order"]


def target_ratios(variable, code_index, station, weekdays=None, time_scale="Hour"):
    code_nr = flat_list(code_index)
    weekday_list = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    if weekdays is not None:
        if set(flat_list(weekdays)) - set(weekday_list) != set():
            raise ValueError(f"Illegal weekday in {weekdays}") from None
        weekday_list = flat_list(weekdays)

    try:
        code_labels = list_items(var_code_labels(variable), code_nr)
    except IndexError:
        raise ValueError(
            f"Illegal code index(es) in {code_nr}, allowed are {var_code_order(variable)}"
        ) from None

    def percent(num):
        return round(num * 100, 1)

    glob_ratio = percent(
        ax_global_ratios(variable)[code_labels].sum(axis="columns").values[0]
    )
    pop_ratio = percent(
        ax_population_ratios(variable)[code_labels].sum(axis="columns").values[0]
    )
    station_counts, sd_ratios = spr_split(
        stations=station,
        weekdays=weekday_list,
        variable=variable,
        time_scale=time_scale,
    )

    observed_codes: list = [
        code for code in code_labels if code in station_counts.columns
    ]
    target_counts = station_counts[observed_codes].sum(axis="columns")
    target_ratios = (
        normalize_rows(station_counts).mul(2)[observed_codes].sum(axis="columns")
    )
    target_data = (
        pd.DataFrame({"Persons": target_counts, "Ratio": target_ratios})
        .eval("CI = sqrt(Persons)")
        .eval("CI_prc = 100 * CI / Persons")
    )

    print(
        f"{var_label(variable)}{code_labels}, bhf = {glob_ratio}%, pop = {pop_ratio}%"
    )
    return target_data


########################################################################################
# MAIN CODE
########################################################################################

code_spec = {"g_220": (0)}  # , "md_ek": [5], "md_hhverm": [5, 6]}
stations = "Zürich Flughafen"
weekdays = all_weekdays
time_scale = "Hour"

result = {}
for (var, code_idx) in code_spec.items():
    for station in flat_list(stations):
        try:
            ratios = target_ratios(
                variable=var,
                code_index=code_idx,
                station=station,
                weekdays=weekdays,
                time_scale=time_scale,
            )
        except NoSPRDataFound:
            warn(f"No SPR+ data found for station '{station}' on weekdays {weekdays}")
            continue
        print(f"{station} ({weekdays})")
        print(ratios.shape)
        result[station] = {
            "Ratios": ratios,
            "Parameters": f"{station} ({weekdays}): {var} ({code_idx})",
        }
