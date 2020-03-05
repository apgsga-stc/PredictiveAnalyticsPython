## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

from dataclasses import dataclass, fields

from pa_lib.file import project_dir, load_bin, load_pickle
from pa_lib.util import list_items

from axinova.UpliftLib import (
    VarId,
    StringList,
    IntList,
    VarCodes,
    StationDef,
    DataFrame,
    VarDict,
    VarStruct,
    VarSelection,
)


########################################################################################
# Data Types
########################################################################################
@dataclass(eq=False)
class UpliftData:
    ax_data: DataFrame = None
    ax_var_struct: DataFrame = None
    spr_data: DataFrame = None
    population_codes: DataFrame = None
    global_codes: DataFrame = None
    station_codes: DataFrame = None
    all_stations: StringList = None
    all_weekdays: StringList = None
    all_timescales: StringList = None
    var_info: VarDict = None

    def __post_init__(self):
        """Link to source data in global object (which gets populated on import)."""
        # Only do this if source_data already exists (not during its own initialization)
        if "SOURCE_DATA" in globals():
            for field in fields(self):
                setattr(self, field.name, getattr(SOURCE_DATA, field.name))

    def __str__(self):
        description = "\n".join(
            [
                f"'ax_data' size: {self.ax_data.shape}",
                f"'ax_var_struct' size: {self.ax_var_struct.shape}",
                f"'spr_data' size: {self.spr_data.shape}",
                f"'population_codes' size: {self.population_codes.shape}",
                f"'global_codes' size: {self.global_codes.shape}",
                f"'station_codes' size: {self.station_codes.shape}",
            ]
        )
        return description

    # Access data objects  #############################################################
    def variable_label(self, var_id: VarId) -> str:
        return self.var_info[var_id]["Label"]

    def variable_code_labels(self, var_id: VarId) -> StringList:
        return self.var_info[var_id]["Codes"]

    def variable_code_order(self, var_id: VarId) -> IntList:
        return self.var_info[var_id]["Order"]

    def variable_table(self, var_id: VarId) -> DataFrame:
        result = DataFrame(
            {
                self.variable_label(var_id): self.variable_code_labels(var_id),
                "Nr": self.variable_code_order(var_id),
            }
        )
        return result

    def ax_population_ratios(self, var_id: VarId) -> DataFrame:
        ratios = self.population_codes.loc[
            self.population_codes["Variable"] == var_id
        ].pivot_table(values="Pop_Ratio", index="Variable", columns="Code")
        return ratios

    def ax_global_ratios(self, var_id: VarId) -> DataFrame:
        ratios = self.global_codes.loc[
            self.global_codes["Variable"] == var_id
        ].pivot_table(values="Ratio", index="Variable", columns="Code")
        return ratios

    def ax_station_ratios(self, var_id: VarId) -> DataFrame:
        ratios = self.station_codes.loc[
            self.station_codes["Variable"] == var_id
        ].pivot_table(values="Ratio", index="Station", columns="Code", fill_value=0)
        return ratios

    # Validate Uplift parameters #######################################################
    def check_stations(self, stations: StationDef) -> StationDef:
        if stations is None or stations == list():
            checked_stations = self.all_stations
        else:
            checked_stations = [
                station for station in stations if station in self.all_stations
            ]
            if len(checked_stations) != len(stations):
                raise ValueError(
                    f"Unknown station found in {stations}, known are: {self.all_stations}"
                )
        return checked_stations

    def check_var_def(self, variables: VarCodes) -> VarSelection:
        checked_var: VarSelection = {}
        for (var_id, code_nr) in variables.items():
            try:
                var_label = self.variable_label(var_id)
            except KeyError:
                raise KeyError(
                    f"Unknown variable '{var_id}', known are {list(self.var_info.keys())}"
                ) from None
            try:
                code_labels = list_items(self.variable_code_labels(var_id), code_nr)
            except IndexError:
                raise ValueError(
                    f"Unknown code index(es) for variable {var_id} in {code_nr}, known are "
                    f"{self.variable_code_order(var_id)}"
                ) from None
            code_order = list_items(self.variable_code_order(var_id), code_nr)
            checked_var[var_id] = VarStruct(var_label, code_labels, code_order)
        return checked_var

    def check_timescale(self, timescale: str) -> str:
        if timescale not in self.all_timescales:
            raise ValueError(
                f"Unknown timescale '{timescale}', known are: {self.all_timescales}"
            )
        return timescale


########################################################################################
# Load data files
########################################################################################
def _load_data() -> UpliftData:
    """Load base data and calculate derived objects. Return data container class."""
    with project_dir("axinova"):
        data = UpliftData(
            ax_data=load_bin("ax_data.feather"),
            ax_var_struct=load_bin("ax_var_struct.feather"),
            population_codes=load_pickle("population_ratios.pkl"),
            global_codes=load_pickle("global_code_ratios.pkl"),
            station_codes=load_pickle("station_code_ratios.pkl"),
            spr_data=load_pickle("spr_data.pkl"),
        )
    data.all_stations = data.ax_data["Station"].cat.categories.to_list()
    data.all_weekdays = data.ax_data["DayOfWeek"].cat.categories.to_list()
    data.all_timescales = ["Time", "ShortTime", "Hour", "TimeSlot"]
    data.var_info = {}
    for (var_id, struct) in data.ax_var_struct.groupby("Variable"):
        data.var_info[var_id] = dict(
            Label=struct["Variable_Label"].max(),
            Codes=struct["Label"].to_list(),
            Order=list(range(len(struct["Label_Nr"].to_list()))),
        )
    data.combi_var = {
        "md_SexAgeEk": (
            data.variable_table("md_SexAgeEk")
            .iloc[:, 0]
            .str.split("/ ", expand=True)
            .rename(columns={0: "md_sex", 1: "md_agenatrep", 2: "md_ek"})
        )
    }
    return data


########################################################################################
# Initialization Code: Load data files on import
########################################################################################
SOURCE_DATA: UpliftData = _load_data()
