## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Tuple
from pprint import pformat


from pa_lib.file import (
    project_dir,
    load_bin,
    load_pickle,
)
from pa_lib.data import clean_up_categoricals
from pa_lib.util import list_items
from pa_lib.log import info, time_log


########################################################################################
# Data Types
########################################################################################
@dataclass(frozen=True)
class VarStruct:
    var_label: str
    code_labels: List[str]
    code_order: List[int]


VarId = str
VarCodes = Dict[VarId, List[int]]
VarSelection = Dict[VarId, VarStruct]
StationDef = List[str]
DataTable = pd.DataFrame
DataSeries = pd.Series
VarResult = Dict[VarId, DataTable]
Result = DataTable


########################################################################################
# Global Data
########################################################################################
ax_data: DataTable
ax_var_struct: DataTable
spr_data: DataTable
population_codes: DataTable
global_codes: DataTable
station_codes: DataTable
all_stations: List[str]
all_weekdays: List[str]
all_timescales: List[str]
var_info: Dict[VarId, dict]


########################################################################################
# Helper functions
########################################################################################
def poisson_sd(data: DataSeries) -> DataSeries:
    return data.pow(0.5)


def combine_sd_ratios(data1: DataSeries, data2: DataSeries) -> DataSeries:
    return (data1.pow(2) + data2.pow(2)).pow(0.5)


########################################################################################
# Load data objects
########################################################################################
def load_data() -> None:
    global ax_data, spr_data, ax_var_struct, var_info
    global population_codes, global_codes, station_codes
    global all_stations, all_weekdays, all_timescales

    with project_dir("axinova"):
        ax_data = load_bin("ax_data.feather")
        ax_var_struct = load_bin("ax_var_struct.feather")
        population_codes = load_pickle("population_ratios.pkl")
        global_codes = load_pickle("global_code_ratios.pkl")
        station_codes = load_pickle("station_code_ratios.pkl")
        spr_data = load_pickle("spr_data.pkl")

    # derived data
    all_stations = ax_data["Station"].cat.categories.to_list()
    all_weekdays = ax_data["DayOfWeek"].cat.categories.to_list()
    all_timescales = ["Time", "ShortTime", "Hour", "TimeSlot"]
    var_info = {}
    for (var_id, data) in ax_var_struct.groupby("Variable"):
        var_info[var_id] = dict(
            Label=data["Variable_Label"].max(),
            Codes=data["Label"].to_list(),
            Order=list(range(len(data["Label_Nr"].to_list()))),
        )


# Accessing data objects
def variable_label(var_id: VarId) -> str:
    return var_info[var_id]["Label"]


def variable_code_labels(var_id: VarId) -> List[str]:
    return var_info[var_id]["Codes"]


def variable_code_order(var_id: VarId) -> List[int]:
    return var_info[var_id]["Order"]


def ax_population_ratios(var_id: VarId) -> DataTable:
    ratios = population_codes.loc[population_codes["Variable"] == var_id].pivot_table(
        values="Pop_Ratio", index="Variable", columns="Code"
    )
    return ratios


def ax_global_ratios(var_id: VarId) -> DataTable:
    ratios = global_codes.loc[global_codes["Variable"] == var_id].pivot_table(
        values="Ratio", index="Variable", columns="Code"
    )
    return ratios


def ax_station_ratios(var_id: VarId) -> DataTable:
    ratios = station_codes.loc[station_codes["Variable"] == var_id].pivot_table(
        values="Ratio", index="Station", columns="Code", fill_value=0
    )
    return ratios


########################################################################################
# Main class
########################################################################################
class Uplift:
    def __init__(
        self, *, name: str, variables: VarCodes, stations: StationDef, time_scale: str
    ) -> None:
        self.name = name
        self.stations = Uplift._check_stations(stations)
        self.variables = Uplift._check_var_def(variables)
        self.time_scale = Uplift._check_timescale(time_scale)
        self._spr_counts = DataSeries()
        self._spr_sd_ratios = DataSeries()
        self._var_result = {}
        self._result = Result()

    def __str__(self) -> str:
        description = "\n".join(
            [
                f"Name: '{self.name}'",
                f"Stations: {self.stations}",
                f"Timescale: '{self.time_scale}'",
                f"Selection: \n{self.selection}",
                f"Results per Variable: \n{pformat(self.var_result)}",
                f"Total Result: \n{pformat(self.result)}",
            ]
        )
        return description

    @property
    def parameters(self) -> dict:
        return {
            "name": self.name,
            "variables": self.variables,
            "stations": self.stations,
            "time_scale": self.time_scale,
        }

    @property
    def result(self) -> Result:
        return self._result

    @property
    def var_result(self) -> VarResult:
        return self._var_result

    def _var_selection(self, var_id: VarId) -> str:
        struct = self.variables[var_id]
        selection = (
            f"'{struct.var_label}' = '" + "' OR '".join(struct.code_labels) + "'"
        )
        return selection

    @property
    def selection(self) -> str:
        selection = "\n AND ".join(
            [self._var_selection(var_id) for var_id in self.variables.keys()]
        )
        return selection

    @staticmethod
    def _build_uplift_columns() -> str:
        return (
            "  pop_uplift     = ax_ratio - pop_ratio    \n"
            + "global_uplift  = ax_ratio - global_ratio \n"
            + "station_uplift = ax_ratio - station_ratio"
        )

    # Parameter validation functions
    @staticmethod
    def _check_stations(stations: StationDef) -> StationDef:
        if stations is None or stations == list():
            checked_stations = all_stations
        else:
            checked_stations = [
                station for station in stations if station in all_stations
            ]
            if len(checked_stations) != len(stations):
                raise ValueError(
                    f"Unknown station found in {stations}, known are: {all_stations}"
                )
        return checked_stations

    @staticmethod
    def _check_var_def(variables: VarCodes) -> VarSelection:
        checked_var: VarSelection = {}
        for (var_id, code_nr) in variables.items():
            try:
                var_label = variable_label(var_id)
            except KeyError:
                raise KeyError(
                    f"Unknown variable '{var_id}', known are {list(var_info.keys())}"
                ) from None
            try:
                code_labels = list_items(variable_code_labels(var_id), code_nr)
            except IndexError:
                raise ValueError(
                    f"Unknown code index(es) for variable {var_id} in {code_nr}, known are "
                    f"{variable_code_order(var_id)}"
                ) from None
            code_order = list_items(variable_code_order(var_id), code_nr)
            checked_var[var_id] = VarStruct(var_label, code_labels, code_order)
        return checked_var

    @staticmethod
    def _check_timescale(timescale: str) -> str:
        if timescale not in all_timescales:
            raise ValueError(
                f"Unknown timescale '{timescale}', known are: {all_timescales}"
            )
        return timescale

    # Aggregate occurrence counts after filtering by selection criteria
    def _get_counts(
        self,
        value_col: str,
        data: DataTable,
        var_id: VarId = None,
        code_labels: List[str] = None,
    ) -> Tuple[DataSeries, DataSeries]:
        select_rows = data["Station"].isin(self.stations)
        if var_id is not None:
            select_rows &= data["Variable"] == var_id
        if code_labels is not None:
            select_rows &= data["Code"].isin(code_labels)
        counts = (
            data.loc[select_rows]
            .pipe(clean_up_categoricals, incl_col="Station")
            .groupby(["Station", "DayOfWeek", self.time_scale])[value_col]
            .agg("sum")
            .fillna(0)
        )
        count_sd = poisson_sd(counts)
        return counts, count_sd

    # Calculation methods
    def _calculate_single_var(self, var_id: VarId) -> DataTable:
        code_labels = self.variables[var_id].code_labels
        ax_total_counts, _ = self._get_counts(
            value_col="Value", data=ax_data, var_id=var_id
        )
        ax_target_counts, ax_target_sd = self._get_counts(
            value_col="Value", data=ax_data, var_id=var_id, code_labels=code_labels
        )
        ax_target_ratios = (ax_target_counts / ax_total_counts).fillna(0)
        target_counts = self._spr_counts * ax_target_ratios
        ax_target_sd_ratios = (ax_target_sd / self._spr_counts).fillna(0)
        target_sd_ratios = combine_sd_ratios(self._spr_sd_ratios, ax_target_sd_ratios)
        target_sd_abs = self._spr_counts * target_sd_ratios

        # reference ratios for CH population / all stations / each station
        pop_ratio = ax_population_ratios(var_id)[code_labels].sum(axis="columns")[0]
        global_ratio = ax_global_ratios(var_id)[code_labels].sum(axis="columns")[0]
        station_ratios = (
            ax_station_ratios(var_id)
            .loc[self.stations, code_labels]
            .sum(axis="columns")
        )

        # collect result table
        result = DataTable(
            {
                "spr": self._spr_counts,
                "spr_sd_ratio": self._spr_sd_ratios,
                "ax_total": ax_total_counts,
                "ax_count": ax_target_counts,
                "ax_ratio": ax_target_ratios,
                "ax_sd_ratio": ax_target_sd_ratios,
                "target_count": target_counts,
                "target_sd_ratio": target_sd_ratios,
                "target_sd_abs": target_sd_abs,
            }
        ).reset_index()
        result = result.assign(
            pop_ratio=pop_ratio,
            global_ratio=global_ratio,
            station_ratio=station_ratios.loc[result["Station"]].values,
        ).eval(self._build_uplift_columns())
        return result

    def calculate(self) -> None:
        # get SPR+ counts for the given selection criteria
        self._spr_counts, spr_sd = self._get_counts(value_col="Total", data=spr_data)
        self._spr_sd_ratios = (spr_sd / self._spr_counts).fillna(0)

        # calculate result per variable
        for var_id in self.variables.keys():
            self._var_result[var_id] = self._calculate_single_var(var_id)

        # assemble total result from variable results
        var_ids = list(self.variables.keys())
        first_var_result = self.var_result[var_ids[0]]
        result = first_var_result[
            [
                "Station",
                "DayOfWeek",
                "Hour",
                "spr",
                "spr_sd_ratio",
                "ax_ratio",
                "target_sd_ratio",
                "pop_ratio",
                "global_ratio",
                "station_ratio",
            ]
        ].copy()
        for var_id in var_ids[1:]:
            var_result = self.var_result[var_id]
            result = result.assign(
                ax_ratio=result["ax_ratio"] * var_result["ax_ratio"],
                target_sd=combine_sd_ratios(
                    result["target_sd_ratio"], var_result["target_sd_ratio"]
                ),
                pop_ratio=result["pop_ratio"] * var_result["pop_ratio"],
                global_ratio=result["global_ratio"] * var_result["global_ratio"],
                station_ratio=result["station_ratio"] * var_result["station_ratio"],
            )
        result = result.assign(
            target_count=result["spr"] * result["ax_ratio"],
            target_sd_abs=result["spr"] * result["target_sd_ratio"],
        )[
            [
                "Station",
                "DayOfWeek",
                "Hour",
                "spr",
                "spr_sd_ratio",
                "ax_ratio",
                "target_sd_ratio",
                "target_count",
                "target_sd_abs",
                "pop_ratio",
                "global_ratio",
                "station_ratio",
            ]
        ].eval(
            self._build_uplift_columns()
        )
        self._result = result


########################################################################################
# INITIALISATION CODE
########################################################################################
with time_log("loading data"):
    load_data()

########################################################################################
# TESTING CODE
########################################################################################
if __name__ == "__main__":
    line = "-" * 88
    print(line)
    with time_log("testing Uplift class"):
        info("-- Initializing instance 'Uplift Test'")
        uplift_test = Uplift(
            name="Uplift Test",
            variables={"g_220": [0, 1], "md_agenatrep": [2, 3]},
            stations=["Aarau", "Brig"],
            time_scale="Hour",
        )
        print(uplift_test)

        print(line)
        info("-- Calculating instance 'Uplift Test'")
        uplift_test.calculate()
        print(uplift_test)

        print(line)
