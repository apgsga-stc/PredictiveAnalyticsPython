## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
from collections import namedtuple
from typing import Dict

from pa_lib.file import project_dir, load_xlsx, store_pickle, cleanup_df
from pa_lib.log import info
from pa_lib.data import as_dtype, dtFactor
from pa_lib.util import cap_words, value


########################################################################################
# Load SPR+ data, enrich with APG digital display info, add columns
########################################################################################
with project_dir("spr_plus"):
    spr_data_raw = (
        load_xlsx("2019_11_13_VSTD_DIGITAL_2019_2020_APG.xlsx", sheet_name="Daten")
        .melt(
            id_vars="WT FLAECHE_ID ANBIETER SPR_FLAECHE_ID".split(),
            var_name="Time",
            value_name="Count",
        )
        .pipe(as_dtype, dtFactor, incl_dtype="object")
        .rename(mapper=lambda name: cap_words(name, sep="_"), axis="columns")
    )

    rail_displays = (
        load_xlsx("flaechen_rail_epanel.xlsx")
        .append(load_xlsx("flaechen_rail_eboard.xlsx"), ignore_index=True, sort=False)
        .drop(["Strasse", "Haus-Nr."], axis="columns")
        .pipe(as_dtype, dtFactor, incl_dtype="object")
    )

# enrich SPR+ data with APG digital display info, hour column, short format time
info("Enrich SPR+ data...")
spr_data_complete = spr_data_raw.merge(
    rail_displays, how="left", left_on="Flaeche_ID", right_on="Nummer"
)
# drop records for non-digital displays
info("Filter SPR+ data...")
spr_data = spr_data_complete.dropna(subset=["PF"])

# add columns for joining with Axinova data: Hour, ShortTima, DayOfWeek
spr_data.loc[:, "Hour"] = spr_data.Time.astype("str").str[:2]
spr_data.loc[:, "ShortTime"] = spr_data.Time.astype("str").str[:5]
spr_data.loc[:, "DayOfWeek"] = spr_data.WT.map(
    {
        "Montag": "Monday",
        "Dienstag": "Tuesday",
        "Mittwoch": "Wednesday",
        "Donnerstag": "Thursday",
        "Freitag": "Friday",
        "Samstag": "Saturday",
        "Sonntag": "Sunday",
    }
).cat.reorder_categories(
    "Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split(), ordered=True,
)


########################################################################################
# Match digital displays to Intervista stations
########################################################################################
NrRange = namedtuple("NrRange", ["low", "high"])
StationMapping = Dict[NrRange, str]
CantonMapping = Dict[str, str]


def map_stations(
    data: pd.DataFrame,
    canton: str = None,
    cantons: CantonMapping = None,
    nr_ranges: StationMapping = None,
) -> pd.DataFrame:
    """
    Map spr_data records to Intervista Station names in-place, either per canton by
    Flaechennummer ranges NrRange(low, high), or by directly mapping from canton
    to station
    """
    mapped_data = data.copy()
    if nr_ranges is not None and canton is not None:
        for nr_range, station_name in nr_ranges.items():
            station_rows = (data["Gebiet Code"] == canton) & (
                data["Nummer"].isin(range(nr_range.low, nr_range.high + 1))
            )
            mapped_data.loc[station_rows, "Station"] = station_name
    elif cantons is not None:
        for canton, station_name in cantons.items():
            station_rows = data["Gebiet Code"] == canton
            mapped_data.loc[station_rows, "Station"] = station_name
    else:
        raise ValueError(
            "Either set parameters 'canton' and 'nr_ranges', or 'cantons'!"
        )
    return mapped_data


info("Map SPR+ data to Intervista Stations...")
spr_data = (
    spr_data.pipe(
        map_stations,
        cantons={
            "AG": "Aarau",
            "BS": "Basel SBB",
            "FR": "Fribourg",
            "GR": "Chur",
            "LU": "Luzern",
            "NE": "Neuchatel",
            "SG": "St. Gallen",
            "SO": "Olten",
            "VS": "Brig",
            "ZG": "Zug",
        },
    )
    .pipe(
        map_stations,
        canton="ZH",
        nr_ranges={
            NrRange(680759, 680831): "Zürich HB",
            NrRange(680846, 680848): "Zürich Stadelhofen",
            NrRange(680858, 680858): "Zürich Enge",
            NrRange(680873, 680876): "Winterthur",
            NrRange(688299, 688303): "Zürich Flughafen",
            NrRange(689938, 690717): "Zürich HB",  # Sihlquai, Gleis-Unterführungen
            NrRange(690723, 690723): "Winterthur",
            NrRange(690910, 690922): "Zürich HB",  # Bahnhofplatz
            NrRange(705698, 705701): "Winterthur",
            NrRange(705702, 705703): "Zürich Stadelhofen",
            NrRange(705704, 705705): "Zürich Oerlikon",
        },
    )
    .pipe(
        map_stations,
        canton="GE",
        nr_ranges={NrRange(680802, 690726): "Genève Cornavin",},
    )
    .pipe(
        map_stations,
        canton="VD",
        nr_ranges={
            NrRange(680797, 711299): "Lausanne",
            NrRange(711300, 711308): "M2",  # M2 Gare Lausanne
            NrRange(711309, 711314): "M2",  # M2 Grancy
        },
    )
    .pipe(
        map_stations,
        canton="BE",
        nr_ranges={
            NrRange(680778, 680862): "Bern",
            NrRange(690699, 690699): "Biel/Bienne",
            NrRange(691900, 691903): "Bern",
            NrRange(711340, 711341): "Biel/Bienne",
            NrRange(721646, 721700): "Bern",
        },
    )
    .pipe(
        map_stations,
        canton="TI",
        nr_ranges={
            NrRange(711180, 711180): "Bellinzona",
            NrRange(713103, 713106): "Lugano",
        },
    )
)

# Check if we matched all of them
info("Check mapping...")
with value(spr_data.loc[spr_data["Station"].isnull()]) as unmatched:
    if unmatched.shape[0] > 0:
        raise ValueError(f"Found {unmatched.shape[0]} unmatched SPR+ records")


########################################################################################
# Store results
########################################################################################
with project_dir("axinova"):
    store_pickle(cleanup_df(spr_data), "spr_data.pkl")
    store_pickle(cleanup_df(spr_data_complete), "spr_data_complete.pkl")