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

from pa_lib.file import (
    store_bin,
    project_dir,
    data_files,
    load_csv,
    load_xlsx,
    store_pickle,
)
from pa_lib.data import (
    as_dtype,
    lookup,
    cut_categorical,
    merge_categories,
    calc_col_partitioned,
)
from pa_lib.log import time_log
from pa_lib.types import dtFactor
from pa_lib.util import list_items


########################################################################################
def load_ax_data(source_dir):
    # how to find & read our data files
    data_pattern = "*Bahnhof_Uhrzeit_*.csv"
    data_params = dict(encoding="cp1252")

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

    with time_log("merging data files"):
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

    return all_data


########################################################################################
def load_ax_var_struct(source_dir):
    label_pattern = "[!~]*VariablenBeschreibung.xlsx"  # if file is open in Excel, there's a ~ shadow file

    with project_dir(source_dir):
        # identify label file
        correct_columns = ["Variable", "Label", "Bemerkung"]
        file_list = data_files(label_pattern)
        if file_list.shape[0] == 0:
            raise FileNotFoundError(
                f"No label file matches '{label_pattern}' in data subdirectory '{source_dir}'"
            )
        elif file_list.shape[0] > 1:
            raise Exception(
                f"Multiple label files match '{label_pattern}' in data subdirectory '{source_dir}'"
            )
        label_file = file_list.index[0]

        # read variable labels
        var_labels = load_xlsx(
            label_file, sheet_name="Variablenbeschreibung", usecols=[0, 1, 2], header=0
        )
        assert (
            var_labels.columns.to_list() == correct_columns
        ), f"Wrong variable labels columns: {var_labels.columns.to_list()}"

        # read code labels
        code_labels = load_xlsx(
            label_file,
            sheet_name="Wertelabels",
            usecols=[0, 2, 3],  # col 1 holds a numeric code
            header=0,
        )
        assert (
            code_labels.columns.to_list() == correct_columns
        ), f"Wrong code label columns: {code_labels.columns.to_list()}"

    # merge variable labels to code labels
    code_labels["Variable_Label"] = lookup(
        var_labels, target_col_name="Label", match_col=code_labels["Variable"]
    )
    var_struct = (
        code_labels.loc[:, "Variable Variable_Label Label".split()]
        .sort_values(["Variable", "Label"])
        .reset_index(drop=True)
    )
    assert all(
        var_struct.isna().sum() == 0
    ), f"Illegal variable structure (has NULLs), check file {label_file}"

    # calculate alphabetic order of codes within variables as "Label_Nr"
    def order(s):
        return s.rank(method="dense").astype("int")

    var_struct = calc_col_partitioned(
        df=var_struct, col="Label_Nr", fun=order, on="Label", part_by="Variable"
    )
    return var_struct


########################################################################################
def fix_code_order(var_struct):
    # define orderings for all variables with given non-alphabetical ordering
    var_codes_ordered = {
        "md_agenatrep": {
            "14-29 Jahre": 1,
            "30-45 Jahre": 2,
            "46-60 Jahre": 3,
            "61+ Jahre": 4,
        },
        "md_bildung3": {
            "niedrig (kein Abschluss, obligat. Schule, HH-Lehrjahr, Handelsschule, Anlehre)": 1,
            "mittel (Diplommittelschule, allg. Schule, Berufslehre, Vollzeitberufsschule, Maturität, "
            "Lehrerseminar)": 2,
            "hoch (Universität, ETH, FH, PH, höhere Berufsausbildung)": 3,
        },
        "md_hhgr3": {"1 Person": 1, "2 Personen": 2, "3+ Personen": 3},
        "g_privatetrainuse": {
            "Nie": 1,
            "1-2 Mal jährlich": 2,
            "3-12 Mal jährlich": 3,
            "2-5 Mal pro Monat": 4,
            "6 Mal pro Monat oder häufiger": 5,
        },
        "md_203": {  # Bahnnutzung beruflich
            "Nie": 1,
            "Seltener": 2,
            "Etwa zu einem Viertel": 3,
            "Etwa zur Hälfte": 4,
            "Meistens": 5,
            "Immer": 6,
        },
        "g_220": {"Keines": 1, "1 Auto": 2, "2+ Autos": 3},  # Autos im Haushalt
        "md_410": {  # Internetnutzung
            "seltener": 1,
            "mehrmals pro Monat": 2,
            "einmal pro Woche": 3,
            "mehrmals pro Woche": 4,
            "täglich/fast täglich": 5,
        },
        # Div.Internet-Nutzungsarten (md_421 bis md_411)
        "md_421": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_419": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_417": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_416": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_415": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_414": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_413": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_412": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_411": {"nie": 1, "gelegentlich": 2, "regelmässig": 3},
        "md_early": {
            "Ich bin immer einer/eine der ersten, der/die neue Technologien und Geräte kauft resp. "
            "einsetzt.": 1,
            "Ich fange erst dann an, neue Technologien und Geräte zu verwenden, wenn ich weiss, "
            "welche Erfahrungen andere mit ihnen gemacht haben.": 2,
            "Ich übernehme neue Technologien und Geräte erst dann, wenn es für mich persönlich oder beruflich "
            "unerlässlich ist.": 3,
        },
        "md_tv": {"kein TV-Gerät": 1, "1 oder mehrere TV-Geräte": 2},
        "g_TvChannelsgroup": {
            "keine Sender": 1,
            "1-4 Sender": 2,
            "5-9 Sender": 3,
            "10++ Sender": 4,
        },
        "g_flug": {
            "keine Flüge": 1,
            "1 - 4 Flüge": 2,
            "5 - 9 Flüge": 3,
            "10++ Flüge": 4,
        },
        "g_flugBusiness": {
            "keine Flüge": 1,
            "1 - 4 Flüge": 2,
            "5 - 9 Flüge": 3,
            "10++ Flüge": 4,
        },
        "md_ek": {
            "Keine Angabe": 0,
            "Weniger als 3'000 CHF": 1,
            "Zwischen 3'000 und 4'500 CHF": 2,
            "Zwischen 4'501 und 6'000 CHF": 3,
            "Zwischen 6'001 und 9'000 CHF": 4,
            "Zwischen 9'001 und 12'000 CHF": 5,
            "Mehr als 12'000 CHF": 6,
        },
        "md_hhverm": {
            "Keine Angabe": 0,
            "Weniger als CHF 50 000": 1,
            "Zwischen CHF 50 000 und CHF 100 000": 2,
            "Zwischen CHF 100 000 und CHF 250 000": 3,
            "Zwischen CHF 250 000 und CHF 500 000": 4,
            "Zwischen CHF 500 000 und CHF 1 Mio.": 5,
            "Zwischen CHF 1 Mio. und CHF 5 Mio.": 6,
            "Mehr als CHF 5 Mio.": 7,
        },
        "md_880": {
            "100": -2,
            "Bürgerlich Demokratische Partei BDP": 7,
            "Christlichdemokratische Volkspartei CVP": 4,
            "Dazu möchte ich keine Angaben machen": -1,
            "Die Grünen GPS / Grünes Bündnis": 2,
            "Eine andere Partei, und zwar:": 9,
            "Evangelische Volkspartei EVP": 5,
            "FDP Die Liberalen": 6,
            "Grünliberale GLP": 3,
            "Keine Partei": 0,
            "Schweizerische Volkspartei SVP": 8,
            "Sozialdemokratische Partei SP": 1,
        },
    }

    # Assign new ordered codes to var_struct for all ordered variables
    # Catch exceptions resulting from data mismatches for manual cleanup
    for var, _ in var_struct.groupby("Variable"):
        if var in var_codes_ordered:
            new_codes = ()
            try:
                new_codes = (
                    pd.DataFrame.from_dict(
                        var_codes_ordered[var], orient="index", columns=["Label_Nr"]
                    )
                    .rename_axis(index="Label")
                    .reset_index()
                    .values
                )
                var_struct.loc[
                    var_struct["Variable"] == var, ["Label", "Label_Nr"]
                ] = new_codes
            except:
                print(f"Problem reordering variable {var}: new codes = \n{new_codes}")
                print("Old structure:")
                print(
                    var_struct.loc[var_struct["Variable"] == var, ["Label", "Label_Nr"]]
                )
                raise

    return var_struct.sort_values(["Variable", "Label_Nr"])


########################################################################################
def convert_ax_data(data, var_struct):
    # Add columns: logValue, Variable description
    result = data.assign(
        logValue=np.log(data["Value"]),
        VarDesc=lookup(
            var_struct, target_col_name="Variable_Label", match_col=data["Variable"]
        ),
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
            left_limits="00:00 06:00 09:00 11:00 13:00 16:00 19:00 23:00".split(),
            labels="Nacht Morgen-Rush Morgen Mittag Nachmittag Abend-Rush Abend Nacht_spät".split(),
            cat_convert=lambda x: x[:5],  # match start time of interval in 'Time'
        )
        .pipe(merge_categories, cat="Nacht_spät", into_cat="Nacht")
    )

    # Add column: Short format time
    result["ShortTime"] = result["Time"].str[:5].astype(dtFactor)

    # Add column: Hour
    result["Hour"] = (result["Time"].str[:2]).astype(dtFactor)

    result = result.sort_values(
        by=["Station", "DayOfWeek", "Time", "Variable", "Code"]
    ).reset_index(drop=True)
    return result


########################################################################################
def enrich_ax_data(data):
    enriched_data = data.copy()

    # categorize weekdays
    weekday_order = data["DayOfWeek"].cat.categories.to_list()
    weekend = weekday_order[5:]
    enriched_data["is_weekend"] = data["DayOfWeek"].isin(weekend)

    # categorize time slots
    time_slot_order = data["TimeSlot"].cat.categories.to_list()
    day = time_slot_order[1:]
    rush_hours = list_items(time_slot_order, [1, 5])
    day_no_rush = list_items(time_slot_order, [2, 3, 4, 6])
    enriched_data["is_day"] = data["TimeSlot"].isin(day)
    enriched_data["is_rush"] = data["TimeSlot"].isin(rush_hours)
    enriched_data["is_day_no_rush"] = data["TimeSlot"].isin(day_no_rush)
    enriched_data.loc[enriched_data["is_rush"], "TimeSlot_cat"] = "Day: Rush Hours"
    enriched_data.loc[
        enriched_data["is_day_no_rush"], "TimeSlot_cat"
    ] = "Day: no Rush Hours"
    enriched_data["TimeSlot_cat"].fillna("Night", inplace=True)

    # categorize stations
    stations_d = [
        "Aarau",
        "Basel SBB",
        "Bern",
        "Biel/Bienne",
        "Brig",
        "Chur",
        "Luzern",
        "Olten",
        "St. Gallen",
        "Winterthur",
        "Zug",
        "Zürich Enge",
        "Zürich Flughafen",
        "Zürich Flughafen - Airside",
        "Zürich Flughafen - Landside",
        "Zürich HB",
        "Zürich Hardbrücke",
        "Zürich Oerlikon",
        "Zürich Stadelhofen",
    ]
    stations_f = [
        "Biel/Bienne",
        "Fribourg",
        "Genève Aéroport",
        "Genève Cornavin",
        "Lausanne",
        "M2",
        "Neuchatel",
    ]
    stations_i = ["Bellinzona", "Lugano"]
    enriched_data.loc[
        data["Station"].isin(stations_f), "StationSprache"
    ] = "Französisch"
    enriched_data.loc[
        data["Station"].isin(stations_i), "StationSprache"
    ] = "Italienisch"
    enriched_data.loc[data["Station"].isin(stations_d), "StationSprache"] = "Deutsch"

    # clean up data types
    enriched_data = as_dtype(enriched_data, dtFactor, incl_dtype=["bool", "object"])

    return enriched_data


########################################################################################
def calculate_global_code_ratios(data):
    global_codes = {}
    
    for time_scale in "ShortTime Hour TimeSlot".split():
        
        # sum value per code for each var/station/time
        global_codes[time_scale] = data.groupby(
            ["Variable", "DayOfWeek", time_scale, "Code"], observed=True, as_index=False
        )["Value"].agg("sum")

        # scale to sum=1 per hour to get ratios per time
        global_codes[time_scale]["Ratio"] = (
            global_codes[time_scale]
            .groupby(["Variable", "DayOfWeek", time_scale], observed=True)["Value"]
            .transform(lambda s: s / s.sum())
        )
    
    return global_codes


########################################################################################
# MAIN CODE
########################################################################################
source_dir = "axinova_month_files"
ax_data = load_ax_data(source_dir)
var_struct = load_ax_var_struct(source_dir)
var_struct = fix_code_order(var_struct)

with time_log("converting data"):
    ax_data = convert_ax_data(ax_data, var_struct)

with time_log("enriching data"):
    ax_data = enrich_ax_data(ax_data)

with time_log("extracting global code ratios"):
    global_code_ratios = calculate_global_code_ratios(ax_data)

with project_dir("axinova"):
    store_bin(ax_data, "ax_data.feather")
    store_bin(var_struct, "ax_var_struct.feather")
    store_pickle(global_code_ratios, "code_ratios.pkl")