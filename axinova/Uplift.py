## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(file_dir))
sys.path.append(str(parent_dir))

import altair as alt
from pprint import pformat
from datetime import datetime as dt

from pa_lib.file import project_dir, store_xlsx
from pa_lib.util import default_dict
from pa_lib.log import info, time_log
from pa_lib.data import clean_up_categoricals

from axinova.UpliftLib import (
    VarId,
    StringList,
    VarCodes,
    VarSelection,
    StationDef,
    DataFrame,
    DataSeries,
    VarResult,
    Result,
    CountsWithSD,
    poisson_sd,
)
from axinova.UpliftData import UpliftData
from axinova.UpliftGraphs import heatmap, barplot


########################################################################################
# Main class
########################################################################################
class Uplift:
    def __init__(
        self, *, name: str, variables: VarCodes, stations: StationDef, time_scale: str
    ) -> None:
        self.name: str = name
        self.data: UpliftData = UpliftData()

        # validate target selection parameters
        self.stations: StationDef = self.data.check_stations(stations)
        self.variables: VarSelection = self.data.check_var_def(variables)
        self.time_scale: str = self.data.check_timescale(time_scale)

        # internal objects
        self._spr: DataSeries = DataSeries(dtype="float")
        self._spr_sd: DataSeries = DataSeries(dtype="float")
        self._spr_min: DataSeries = DataSeries(dtype="float")
        self._spr_max: DataSeries = DataSeries(dtype="float")
        self._spr_sd_ratio: DataSeries = DataSeries(dtype="float")
        self._var_result: VarResult = dict()
        self._result: Result = Result()

    def __str__(self) -> str:
        description = "\n".join(
            [
                f"Name: '{self.name}'",
                f"Stations: {self.stations}",
                f"Timescale: '{self.time_scale}'",
                f"Selection: \n{self.selection}",
                f"Source data: \n{self.data}",
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

    def var_selection(self, var_id: VarId) -> str:
        struct = self.variables[var_id]
        selection = (
            f"'{struct.var_label}' = '" + "' OR '".join(struct.code_labels) + "'"
        )
        return selection

    @property
    def selection(self) -> str:
        selection = "\n AND ".join(
            [self.var_selection(var_id) for var_id in self.variables.keys()]
        )
        return selection

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
            value_col="Value", data=self.data.ax_data, var_id=var_id
        )
        full_index = ax_total_count.index
        ax_pers_count, ax_pers_sd = self._get_counts(
            value_col="Value",
            data=self.data.ax_data,
            var_id=var_id,
            code_labels=code_labels,
        )
        target_ratio = (ax_pers_count / ax_total_count).fillna(0)
        target_pers = target_ratio * self._spr

        # reference ratios for CH population / all stations / each station
        pop_ratio = self.data.ax_population_ratios(var_id)[code_labels].sum(
            axis="columns"
        )[0]
        global_ratio = self.data.ax_global_ratios(var_id)[code_labels].sum(
            axis="columns"
        )[0]
        station_ratios = (
            self.data.ax_station_ratios(var_id)
            .loc[self.stations, code_labels]
            .sum(axis="columns")
        )

        # collect result table
        result = (
            DataFrame(
                {
                    "spr": self._spr,
                    "spr_sd": self._spr_sd,
                    "spr_sd_ratio": self._spr_sd_ratio,
                    "ax_total": ax_total_count,
                    "ax_pers": ax_pers_count,
                    "target_ratio": target_ratio,
                    "target_pers": target_pers,
                }
            )
            .reindex(full_index)
            .fillna(0)
        )
        result = result.assign(
            pop_ratio=pop_ratio,
            global_ratio=global_ratio,
            station_ratio=station_ratios.loc[
                result.index.get_level_values("Station")
            ].values,
        ).build_uplift_columns()
        return result

    def calculate(self) -> None:
        """Calculate target group ratios per variable and overall."""
        # get SPR+ counts for the given selection criteria
        self._spr, self._spr_sd = self._get_counts(
            value_col="Total", data=self.data.spr_data
        )
        self._spr_sd_ratio = ((self._spr + self._spr_sd) / self._spr - 1).fillna(0)

        # calculate result per variable
        for var_id in self.variables.keys():
            self._var_result[var_id] = self._calculate_single_var(var_id)

        # assemble total result from variable results
        var_ids = list(self.variables.keys())
        first_var_result = self.var_result[var_ids[0]]
        result = first_var_result[
            (
                f"spr spr_sd spr_sd_ratio target_ratio target_pers"
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
            result = result.assign(target_pers=result["spr"] * result["target_ratio"])[
                "spr target_ratio target_pers pop_ratio global_ratio station_ratio".split()
            ].build_uplift_columns()
        self._result = result

    ## Result export  ##################################################################
    def export_result(self):
        export_file_name = f"{self.name} {dt.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with project_dir("axinova/zielgruppen_export"):
            store_xlsx(df=self.result, file_name=export_file_name, sheet_name="result")

    ## Visualisation methods ###########################################################
    def heatmap(
        self, selectors: dict = None, plot_properties: dict = None
    ) -> alt.Chart:
        if selectors is None:
            selectors = {}
        if plot_properties is None:
            plot_properties = {}

        properties = default_dict(plot_properties, defaults={"width": 600})
        chart = heatmap(
            data=self.result,
            selectors=selectors,
            title=f"{self.name}: Uplift vs. CH population",
            time_scale=self.time_scale,
            properties=properties,
        )
        return chart

    def plot_pop_uplift(
        self, selectors: dict = None, plot_properties: dict = None, axes: str = "shared"
    ) -> alt.Chart:
        if selectors is None:
            selectors = {}
        if plot_properties is None:
            plot_properties = {}

        properties = default_dict(
            plot_properties, defaults={"width": 200, "height": 300}
        )
        chart = barplot(
            data=self.result,
            selectors=selectors,
            title=f"{self.name}: Uplift vs. CH population",
            time_scale=self.time_scale,
            axes=axes,
            properties=properties,
        )
        return chart


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
            stations=[],  # ["Aarau", "Brig"],
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
        test_heatmap = uplift_test.heatmap(selectors={})
        test_barplot = uplift_test.plot_pop_uplift(selectors={"Station": "Aarau"})

        print(line)
