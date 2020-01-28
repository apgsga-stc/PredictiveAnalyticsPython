## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
import altair as alt
from pprint import pformat
from dataclasses import dataclass
from typing import List, Dict, Tuple
from datetime import datetime as dt

from pa_lib.file import project_dir, load_bin, load_pickle, store_xlsx
from pa_lib.util import list_items
from pa_lib.log import info, time_log
from pa_lib.data import clean_up_categoricals, select_rows, as_dtype


########################################################################################
# Data Types
########################################################################################
VarId = str
StringList = List[str]
IntList = List[int]
VarCodes = Dict[VarId, IntList]
StationDef = StringList
DataFrame = pd.DataFrame
DataSeries = pd.Series
VarResult = Dict[VarId, DataFrame]
VarDict = Dict[VarId, dict]
Result = DataFrame
CountsWithSD = Tuple[DataSeries, DataSeries]


@dataclass(frozen=True)
class VarStruct:
    var_label: str
    code_labels: StringList
    code_order: StringList


VarSelection = Dict[VarId, VarStruct]

########################################################################################
# Global Data
########################################################################################
ax_data: DataFrame
ax_var_struct: DataFrame
spr_data: DataFrame
population_codes: DataFrame
global_codes: DataFrame
station_codes: DataFrame
all_stations: StringList
all_weekdays: StringList
all_timescales: StringList
var_info: VarDict


########################################################################################
# Helper functions
########################################################################################
def poisson_sd(data: DataSeries) -> DataSeries:
    """Return poisson standard deviations for a series of counts => sqrt(count)."""
    return data.pow(0.5)


def combine_sd_ratios(data1: DataSeries, data2: DataSeries) -> DataSeries:
    """Aggregate standard deviations of two independent var => sqrt(sd1^2 + sd2^2)."""
    return (data1.pow(2) + data2.pow(2)).pow(0.5)


def plusminus(data1: DataSeries, data2: DataSeries) -> Tuple[DataSeries, DataSeries]:
    return data1 + data2, data1 - data2


# patch pd.Series with convenience method "plusminus"
DataSeries.plusminus = plusminus


def build_uplift_columns(df: DataFrame) -> DataFrame:
    return df.eval(
        "\n".join(
            [
                "pop_uplift       = target_ratio - pop_ratio    ",
                "global_uplift    = target_ratio - global_ratio ",
                "station_uplift   = target_ratio - station_ratio",
                "pop_uplift_pers  = spr * pop_uplift            ",
                "glob_uplift_pers = spr * global_uplift         ",
                "stat_uplift_pers = spr * station_uplift        ",
            ]
        )
    )


# patch pd.DataFrame with convenience method "build_uplift_columns"
DataFrame.build_uplift_columns = build_uplift_columns


########################################################################################
# Load data objects
########################################################################################
def load_data() -> None:
    """Load base data and calculate derived objects. All objects are module-global."""
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


########################################################################################
# Access data objects
########################################################################################
def variable_label(var_id: VarId) -> str:
    return var_info[var_id]["Label"]


def variable_code_labels(var_id: VarId) -> StringList:
    return var_info[var_id]["Codes"]


def variable_code_order(var_id: VarId) -> IntList:
    return var_info[var_id]["Order"]


def ax_population_ratios(var_id: VarId) -> DataFrame:
    ratios = population_codes.loc[population_codes["Variable"] == var_id].pivot_table(
        values="Pop_Ratio", index="Variable", columns="Code"
    )
    return ratios


def ax_global_ratios(var_id: VarId) -> DataFrame:
    ratios = global_codes.loc[global_codes["Variable"] == var_id].pivot_table(
        values="Ratio", index="Variable", columns="Code"
    )
    return ratios


def ax_station_ratios(var_id: VarId) -> DataFrame:
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
        self._spr = DataSeries()
        self._spr_sd = DataSeries()
        self._spr_min = DataSeries()
        self._spr_max = DataSeries()
        self._spr_sd_ratio = DataSeries()
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

    ## Initialization parameter validation functions ###################################
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

    ## Calculation methods #############################################################
    def _get_counts(
        self,
        value_col: str,
        data: DataFrame,
        var_id: VarId = None,
        code_labels: StringList = None,
    ) -> CountsWithSD:
        """Aggregate occurrence counts after filtering by selection criteria.
        Return counts and Poisson standard deviations."""
        selected_rows = data["Station"].isin(self.stations)
        if var_id is not None:
            selected_rows &= data["Variable"] == var_id
        if code_labels is not None:
            selected_rows &= data["Code"].isin(code_labels)
        counts = (
            data.loc[selected_rows]
            .pipe(clean_up_categoricals, incl_col="Station")
            .groupby(["Station", "DayOfWeek", self.time_scale])[value_col]
            .agg("sum")
            .fillna(0)
        )
        count_sd = poisson_sd(counts)
        return counts, count_sd

    def _calculate_single_var(self, var_id: VarId) -> DataFrame:
        """Calculate target group ratios for a single variable."""
        code_labels = self.variables[var_id].code_labels
        ax_total_count, _ = self._get_counts(
            value_col="Value", data=ax_data, var_id=var_id
        )
        ax_pers_count, ax_pers_sd = self._get_counts(
            value_col="Value", data=ax_data, var_id=var_id, code_labels=code_labels
        )
        target_ratio = (ax_pers_count / ax_total_count).fillna(0)
        target_pers = self._spr * target_ratio

        # reference ratios for CH population / all stations / each station
        pop_ratio = ax_population_ratios(var_id)[code_labels].sum(axis="columns")[0]
        global_ratio = ax_global_ratios(var_id)[code_labels].sum(axis="columns")[0]
        station_ratios = (
            ax_station_ratios(var_id)
            .loc[self.stations, code_labels]
            .sum(axis="columns")
        )

        # collect result table
        result = DataFrame(
            {
                "spr": self._spr,
                "spr_sd": self._spr_sd,
                "spr_sd_ratio": self._spr_sd_ratio,
                "ax_total": ax_total_count,
                "ax_pers": ax_pers_count,
                "target_ratio": target_ratio,
                "target_pers": target_pers,
            }
        ).reset_index()
        result = result.assign(
            pop_ratio=pop_ratio,
            global_ratio=global_ratio,
            station_ratio=station_ratios.loc[result["Station"]].values,
        ).build_uplift_columns()
        return result

    def calculate(self) -> None:
        """Calculate target group ratios per variable and overall."""
        # get SPR+ counts for the given selection criteria
        self._spr, self._spr_sd = self._get_counts(value_col="Total", data=spr_data)
        self._spr_sd_ratio = ((self._spr + self._spr_sd) / self._spr - 1).fillna(0)

        # calculate result per variable
        for var_id in self.variables.keys():
            self._var_result[var_id] = self._calculate_single_var(var_id)

        # assemble total result from variable results
        var_ids = list(self.variables.keys())
        first_var_result = self.var_result[var_ids[0]]
        result = first_var_result[
            (
                f"Station DayOfWeek {self.time_scale} spr spr_sd spr_sd_ratio target_ratio target_pers"
                + " pop_ratio global_ratio station_ratio"
            ).split()
        ].copy()
        if len(var_ids) > 1:
            for var_id in var_ids[1:]:
                vr = self.var_result[var_id]
                result = result.assign(
                    target_ratio=result["target_ratio"] * vr["target_ratio"],
                    pop_ratio=result["pop_ratio"] * vr["pop_ratio"],
                    global_ratio=result["global_ratio"] * vr["global_ratio"],
                    station_ratio=result["station_ratio"] * vr["station_ratio"],
                )
            result = result.assign(target_pers=result["spr"] * result["target_ratio"],)[
                (
                    f"Station DayOfWeek {self.time_scale} spr target_ratio"
                    + " target_pers"
                    + " pop_ratio global_ratio station_ratio"
                ).split()
            ].build_uplift_columns()
        self._result = result

    ## Result export  ##################################################################
    def export_result(self):
        export_file_name = f"{self.name} {dt.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with project_dir("axinova/zielgruppen_export"):
            store_xlsx(df=self.result, file_name=export_file_name, sheet_name="result")

    ## Visualisation methods ###########################################################
    def heatmap(self):
        pass

    def plot_pop_uplift(
        self, selectors: dict = None, plot_properties: dict = None
    ) -> alt.Chart:
        if selectors is None:
            selectors = {}
        chart_data = select_rows(self.result, selectors).pipe(
            as_dtype, "int", incl_pattern="spr|.*pers$"
        )

        if plot_properties is None:
            plot_properties = {}
        properties = {"width": 200, "height": 200}
        properties.update(plot_properties)
        chart = (
            alt.Chart(chart_data)
            .properties(**properties)
            .mark_bar()
            .encode(
                x=alt.X(
                    "pop_uplift_pers:Q", title="", scale=alt.Scale(type="linear")
                ),  # type="symlog"|"linear"
                y=alt.Y(
                    f"{self.time_scale}:O",
                    axis=alt.Axis(grid=True),
                    sort=ax_data[self.time_scale].cat.categories.to_list(),
                ),
                tooltip=[self.time_scale, "spr", "target_pers", "pop_uplift_pers"],
                color=alt.condition(
                    alt.datum.pop_uplift_pers > 0,
                    alt.value("steelblue"),  # The positive color
                    alt.value("orange"),  # The negative color
                ),
                row=alt.Row("DayOfWeek", title="Wochentag", sort=all_weekdays,),
                column=alt.Column("Station", title="Bahnhof"),
            )
        )
        return chart


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
        info("-- Exporting result to XLSX file")
        uplift_test.export_result()

        print(line)
        info("-- Plotting result: Uplift vs. population")
        plot = uplift_test.plot_pop_uplift(selectors={"Station": "Aarau"})
        plot.serve()

        print(line)
