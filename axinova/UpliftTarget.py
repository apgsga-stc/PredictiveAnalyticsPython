## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import altair as alt
import re
import numpy as np
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Callable, TypeVar, Any
from datetime import datetime as dt
from textwrap import indent as indent_by

from pa_lib.util import list_items, default_dict
from pa_lib.data import clean_up_categoricals, unfactorize, as_dtype
from pa_lib.file import project_dir, store_xlsx
from axinova.UpliftData import UpliftData, SOURCE_DATA
from axinova.UpliftGraphs import heatmap, barplot, station_heatmap
from axinova.UpliftLib import (
    VarId,
    IntList,
    StringList,
    DataFrame,
    DataSeries,
    poisson_sd,
)

########################################################################################
# TYPES
########################################################################################
Node = TypeVar("Node")

########################################################################################
# GLOBAL OBJECTS
########################################################################################
_spr_data_cache: Dict[tuple, Tuple[DataSeries, DataSeries, DataSeries]] = {}

source_data: UpliftData = SOURCE_DATA


########################################################################################
# HELPER FUNCTIONS
########################################################################################
def _validate_target(target: "_Target") -> None:
    assert isinstance(
        target, _Target
    ), f"target of wrong type {target.__class__}, must be one of: Variable, Or, And"


def _get_counts(
    value_col: str,
    data: DataFrame,
    stations: StringList,
    timescale: str,
    variable: VarId = None,
    code_labels: StringList = None,
) -> Tuple[DataSeries, DataSeries, DataSeries]:
    """Sum occurrence counts after filtering by selection criteria.
    Return counts and Poisson standard deviations."""
    if variable is not None:
        data = data.query("Variable == @variable")
    if len(stations) > 0:
        data = data.query("Station == @stations").pipe(
            clean_up_categoricals, incl_col="Station"
        )
    if code_labels is not None:
        data = data.query("Code == @code_labels")
    counts = (
        data.groupby(["Station", "DayOfWeek", timescale])[value_col]
        .agg("sum")
        .fillna(0)
    )
    count_sd = poisson_sd(counts)
    count_sd_ratio = ((counts + count_sd) / counts - 1).fillna(0)
    return counts, count_sd, count_sd_ratio


def _aggregate_spr_data(
    spr_data: DataFrame, stations: StringList, timescale: str
) -> Tuple[DataSeries, DataSeries, DataSeries]:
    cache_tag = (timescale,) + tuple(stations)
    try:
        return _spr_data_cache[cache_tag]
    except KeyError:
        spr_pers, spr_sd, spr_sd_ratio = _get_counts(
            value_col="Total", data=spr_data, stations=stations, timescale=timescale
        )
        _spr_data_cache[cache_tag] = (spr_pers, spr_sd, spr_sd_ratio)
        return spr_pers, spr_sd, spr_sd_ratio


def _make_file_name(file_name: str) -> str:
    """Clean up file name (remove non-alphanumerics and space runs)"""
    new_name = re.sub(r" +", " ", re.sub(r"[^A-Za-z0-9]", "", file_name))
    return new_name


########################################################################################
# CLASSES
########################################################################################
@dataclass
class _Target(ABC):
    name: str
    _stations: StringList = field(init=False, default_factory=list)
    _timescale: str = field(init=False, default="Hour")
    result: DataFrame = field(init=False, repr=False)
    data: UpliftData = field(init=False, default_factory=UpliftData, repr=False)

    def __post_init__(self):
        self._validate()

    def stations(self, empty_means_all: bool = True) -> StringList:
        if empty_means_all and self._stations == list():
            return self.data.all_stations
        return self._stations

    @abstractmethod
    def set_stations(self, stations: StringList) -> None:
        pass

    @property
    def timescale(self) -> str:
        return self._timescale

    @abstractmethod
    def set_timescale(self, timescale: str) -> None:
        pass

    @abstractmethod
    def _validate(self) -> None:
        pass

    @abstractmethod
    def calculate(self) -> None:
        pass

    @abstractmethod
    def description(self, indent: str = "") -> str:
        pass

    @property
    @abstractmethod
    def node_list(self) -> List[Node]:
        pass

    ## Result output  ##################################################################
    def result_summary(self) -> DataFrame:
        summary = DataFrame(
            dict(ratio=[self.result["pop_ratio"][0], self.result["global_ratio"][0]]),
            index=["Population", "All Stations"],
        )
        return summary

    def best_stations(self, where: str = "") -> DataFrame:
        if where:
            data = self.result.query(where)
        else:
            data = self.result
        station_table = (
            data.groupby("Station")[["spr", "target_pers"]]
            .agg("sum")
            .eval("ratio = target_pers / spr")
            .fillna(0)
            .sort_values("ratio", ascending=False)
            .pipe(as_dtype, "int", incl_col=["spr", "target_pers"])
            .reset_index()
            .pipe(unfactorize)
            .set_index("Station")
        )
        return station_table

    def best_station_days(self, where: str = "") -> DataFrame:
        if where:
            data = self.result.query(where)
        else:
            data = self.result
        station_day_table = (
            data.reset_index()
            .groupby(["Station", "DayOfWeek"], observed=True, as_index=False)[
                ["spr", "ax_total", "ax_pers"]
            ]
            .agg("sum")
            .pipe(unfactorize)
            .fillna(0)
        )
        station_day_table = station_day_table.assign(
            target_ratio=(station_day_table["ax_pers"] / station_day_table["ax_total"]),
            ax_sd_ratio=(
                np.sqrt(station_day_table["ax_pers"]) / station_day_table["ax_pers"]
            ),
            spr_sd_ratio=(np.sqrt(station_day_table["spr"]) / station_day_table["spr"]),
        ).fillna(0)
        station_day_table = (
            station_day_table.assign(
                target_pers=station_day_table["target_ratio"]
                * station_day_table["spr"],
                target_pers_sd_ratio=np.sqrt(
                    np.power(station_day_table["spr_sd_ratio"], 2)
                    + np.power(station_day_table["ax_sd_ratio"], 2)
                ),
            )
            .pipe(
                as_dtype, "int", incl_col=["spr", "ax_total", "ax_pers", "target_pers"]
            )
            .pipe(unfactorize)
            .set_index(["Station", "DayOfWeek"])
        )
        return station_day_table

    def best_slots(
        self, column: Any, top_n: int = 20, where: str = "", show_col: List[str] = None
    ) -> DataFrame:
        if where:
            data = self.result.query(where)
        else:
            data = self.result
        if show_col is None or show_col == list():
            show_col = [
                "spr",
                "ax_pers",
                "target_ratio",
                "target_pers",
                "target_pers_sd_ratio",
                "pop_uplift_ratio",
                "pop_uplift_pers",
            ]
        data = (
            data.sort_values(column, ascending=False)
            .head(top_n)[show_col]
            .pipe(as_dtype, "int", incl_col=["spr", "target_pers", "pop_uplift_pers"])
            .reset_index()
            .pipe(unfactorize)
            .set_index(["Station", "DayOfWeek", "Hour"])
        )
        return data

    ## Result export  ##################################################################
    def export_result(self):
        export_file_name = (
            f"{_make_file_name(self.name)} {dt.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        sheets = {_make_file_name(node.name): node.result for node in self.node_list}
        with project_dir("axinova/zielgruppen_export"):
            store_xlsx(df=DataFrame(), file_name=export_file_name, sheets=sheets)

    ## Visualisation methods ###########################################################
    def plot_ch_uplift_heatmap(
        self,
        selectors: dict = None,
        target_col: str = "pop_uplift_pers",
        plot_properties: dict = None,
    ) -> alt.Chart:
        if selectors is None:
            selectors = {}
        if plot_properties is None:
            plot_properties = {}

        properties = default_dict(
            plot_properties, defaults=dict(width=600, background="white")
        )
        chart = heatmap(
            data=self.result,
            selectors=selectors,
            title=f"{self.name}: Uplift vs. CH population",
            timescale=self.timescale,
            target_col=target_col,
            target_title=target_col,
            properties=properties,
        )
        return chart

    def plot_ch_uplift_barplot(
        self,
        selectors: dict = None,
        target_col: str = "pop_uplift_pers",
        target_ci_col: str = "target_pers_sd",
        target_threshold: float = 0,
        plot_properties: dict = None,
        axes: str = "independent",
    ) -> alt.Chart:
        if selectors is None:
            selectors = {}
        if plot_properties is None:
            plot_properties = {}

        properties = default_dict(
            plot_properties, defaults=dict(width=200, height=300, background="white")
        )
        chart = barplot(
            data=self.result,
            selectors=selectors,
            title=f"{self.name}: Uplift vs. CH population",
            timescale=self.timescale,
            target_col=target_col,
            target_ci_col=target_ci_col,
            target_threshold=target_threshold,
            target_title=target_col,
            axes=axes,
            properties=properties,
        )
        return chart

    def plot_target_pers_heatmap(
        self,
        selectors: dict = None,
        target_col: str = "target_pers",
        plot_properties: dict = None,
    ) -> alt.Chart:
        if selectors is None:
            selectors = {}
        if plot_properties is None:
            plot_properties = {}

        properties = default_dict(
            plot_properties, defaults=dict(width=600, background="white")
        )
        chart = heatmap(
            data=self.result,
            selectors=selectors,
            title=f"{self.name}: Personen",
            timescale=self.timescale,
            target_col=target_col,
            target_title=target_col,
            color_range=["white", "darkgreen"],
            properties=properties,
        )
        return chart

    def plot_station_heatmap(
        self,
        selectors: dict = None,
        target_col: str = "target_ratio",
        plot_properties: dict = None,
    ) -> alt.Chart:
        if selectors is None:
            selectors = {}
        if plot_properties is None:
            plot_properties = {}

        properties = default_dict(
            plot_properties, defaults=dict(width=400, height=800, background="white")
        )
        chart = station_heatmap(
            data=self.result,
            selectors=selectors,
            title=f"{self.name}: Prozent",
            target_col=target_col,
            target_title="Zielgruppe [%]",
            properties=properties,
        )
        return chart


@dataclass
class Variable(_Target):
    variable: VarId
    code_nr: IntList
    var_label: str = field(init=False)
    code_labels: StringList = field(init=False)
    code_order: IntList = field(init=False)

    def __post_init__(self):
        (self.var_label, self.code_labels, self.code_order) = self._validate()

    def _validate(self) -> Tuple[str, StringList, IntList]:
        try:
            var_label = self.data.variable_label(self.variable)
        except KeyError:
            raise KeyError(
                f"Unknown variable '{self.variable}', known are {list(self.data.var_info.keys())}"
            ) from None
        try:
            code_labels = list_items(
                self.data.variable_code_labels(self.variable), self.code_nr
            )
        except IndexError:
            raise ValueError(
                f"Unknown code index(es) for variable {self.variable} in {self.code_nr}, known are "
                f"{self.data.variable_code_order(self.variable)}"
            ) from None
        code_order = list_items(
            self.data.variable_code_order(self.variable), self.code_nr
        )
        return var_label, code_labels, code_order

    def set_stations(self, stations: StringList) -> None:
        if stations is None or stations == list():
            checked_stations = list()
        else:
            checked_stations = [
                station for station in stations if station in self.data.all_stations
            ]
            if len(checked_stations) != len(stations):
                raise ValueError(
                    f"Unknown station found in {stations}, known are: {self.data.all_stations}"
                )
        self._stations = checked_stations

    def set_timescale(self, timescale: str) -> None:
        if timescale not in self.data.all_timescales:
            raise ValueError(
                f"Unknown timescale '{timescale}', known are: {self.data.all_timescales}"
            )
        self._timescale = timescale

    def calculate(self) -> None:
        """Calculate target group ratios for a single variable."""
        spr_pers, spr_sd, spr_sd_ratio = _aggregate_spr_data(
            self.data.spr_data, self.stations(empty_means_all=False), self.timescale
        )
        ax_total_count, _, _ = _get_counts(
            value_col="Value",
            data=self.data.ax_data,
            stations=self.stations(),
            timescale=self.timescale,
            variable=self.variable,
        )
        ax_pers_count, ax_pers_sd, ax_pers_sd_ratio = _get_counts(
            value_col="Value",
            data=self.data.ax_data,
            stations=self.stations(),
            timescale=self.timescale,
            variable=self.variable,
            code_labels=self.code_labels,
        )
        target_ratio = (ax_pers_count / ax_total_count).fillna(0)
        target_pers = target_ratio * spr_pers
        target_pers_sd_ratio = np.sqrt(
            np.power(ax_pers_sd_ratio, 2) + np.power(spr_sd_ratio, 2)
        )
        target_pers_sd = target_pers * target_pers_sd_ratio

        # reference ratios for CH population / all _stations / each station
        pop_ratio = self.data.ax_population_ratios(self.variable)[self.code_labels].sum(
            axis="columns"
        )[0]
        global_ratio = self.data.ax_global_ratios(self.variable)[self.code_labels].sum(
            axis="columns"
        )[0]
        station_ratios = (
            self.data.ax_station_ratios(self.variable)
            .loc[self.stations(), self.code_labels]
            .sum(axis="columns")
        )

        # collect result table
        result = (
            DataFrame(
                {
                    "spr": spr_pers,
                    "spr_sd": spr_sd,
                    "spr_sd_ratio": spr_sd_ratio,
                    "ax_total": ax_total_count,
                    "ax_pers": ax_pers_count,
                    "ax_pers_sd": ax_pers_sd,
                    "ax_pers_sd_ratio": ax_pers_sd_ratio,
                    "target_ratio": target_ratio,
                    "target_pers": target_pers,
                    "target_pers_sd": target_pers_sd,
                    "target_pers_sd_ratio": target_pers_sd_ratio,
                }
            )
            .reindex(ax_total_count.index)  # include all possible variants
            .fillna(0)
        )
        result = result.assign(
            pop_ratio=pop_ratio,
            global_ratio=global_ratio,
            station_ratio=station_ratios.loc[
                result.index.get_level_values("Station")
            ].values,
        ).build_uplift_columns()
        self.result = result

    def description(self, indent: str = "") -> str:
        description = (
            f"{self.name} = '{self.var_label}' IN [\n    '"
            + "',\n    '".join(self.code_labels)
            + "'\n]"
        )
        return description

    @property
    def node_list(self) -> List[Node]:
        return [self]


@dataclass
class _TargetCombination(_Target, ABC):
    target1: _Target
    target2: _Target

    @abstractmethod
    def calculate(self) -> None:
        pass

    @abstractmethod
    def description(self, indent: str = "") -> str:
        pass

    def _validate(self) -> None:
        _validate_target(self.target1)
        _validate_target(self.target2)

    def set_stations(self, stations: StringList) -> None:
        self.target1.set_stations(stations)
        self.target2.set_stations(stations)
        self._stations = stations

    def set_timescale(self, timescale: str) -> None:
        self.target1.set_timescale(timescale)
        self.target2.set_timescale(timescale)
        self._timescale = timescale

    def calculate_combination(
        self, combine_ratios: Callable[[DataSeries, DataSeries], DataSeries]
    ) -> None:
        def combine_sd_ratios(
            sd_ratio1: DataSeries, sd_ratio2: DataSeries
        ) -> DataSeries:
            return np.sqrt(np.power(sd_ratio1, 2) + np.power(sd_ratio2, 2))

        self.target1.calculate()
        self.target2.calculate()
        result1, result2 = self.target1.result, self.target2.result
        result = DataFrame(
            dict(
                spr=result1["spr"],
                spr_sd=result1["spr_sd"],
                spr_sd_ratio=result1["spr_sd_ratio"],
                ax_total=result1["ax_total"],
                target_ratio=combine_ratios(
                    result1["target_ratio"], result2["target_ratio"]
                ),
                pop_ratio=combine_ratios(result1["pop_ratio"], result2["pop_ratio"]),
                global_ratio=combine_ratios(
                    result1["global_ratio"], result2["global_ratio"]
                ),
                station_ratio=combine_ratios(
                    result1["station_ratio"], result2["station_ratio"]
                ),
                target_pers_sd_ratio=combine_sd_ratios(
                    result1["target_pers_sd_ratio"], result2["target_pers_sd_ratio"]
                ),
            )
        )
        result = result.assign(
            target_pers=result["spr"] * result["target_ratio"],
            ax_pers=result["ax_total"] * result["target_ratio"],
        ).build_uplift_columns()
        self.result = result

    def describe_combination(self, kind: str, indent: str = "") -> str:
        desc1 = self.target1.description(f"{indent}  ")
        desc2 = self.target2.description(f"{indent}  ")
        description = "\n".join(
            [
                f"{self.name} = (",
                indent_by(desc1, f"{indent}  "),
                indent_by(kind, indent),
                indent_by(desc2, f"{indent}  "),
                indent_by(")", indent),
            ]
        )
        return description

    @property
    def node_list(self) -> List[Node]:
        return [self] + self.target1.node_list + self.target2.node_list


@dataclass
class And(_TargetCombination):
    @staticmethod
    def _and_ratio(ratio1: DataSeries, ratio2: DataSeries) -> DataSeries:
        return ratio1 * ratio2

    def calculate(self) -> None:
        _TargetCombination.calculate_combination(self, combine_ratios=self._and_ratio)

    def description(self, indent: str = "") -> str:
        description = _TargetCombination.describe_combination(self, "AND", indent)
        return description


@dataclass
class Or(_TargetCombination):
    @staticmethod
    def _or_ratio(ratio1: DataSeries, ratio2: DataSeries) -> DataSeries:
        return ratio1 + ratio2 - (ratio1 * ratio2)

    def calculate(self) -> None:
        _TargetCombination.calculate_combination(self, combine_ratios=self._or_ratio)

    def description(self, indent: str = "") -> str:
        description = _TargetCombination.describe_combination(self, "OR", indent)
        return description


########################################################################################
# TESTING CODE
########################################################################################
if __name__ == "__main__":
    from pa_lib.log import time_log

    line = "-" * 88

    print(line)
    TargetJungHoch = Variable(
        "Jung und hohes Einkommen", variable="md_SexAgeEk", code_nr=[4, 5, 32, 33]
    )
    print(TargetJungHoch.description())
    TargetJungHoch.set_timescale("Hour")
    TargetJungHoch.calculate()

    print(line)
    TargetKMU = Variable("KMU", variable="md_920", code_nr=[1])
    print(TargetKMU.description())
    TargetKMU.set_timescale("Hour")
    TargetKMU.calculate()

    print(line)
    wenig_vermoegen = Variable(
        name="Wenig Vermögen", variable="md_hhverm", code_nr=[0, 1]
    )
    wenig_einkommen = Variable(name="Wenig Einkommen", variable="md_ek", code_nr=[0, 1])
    maennlich = Variable(name="Männlich", variable="md_sex", code_nr=[0])
    unterschicht = Or("Unterschicht", wenig_vermoegen, wenig_einkommen)
    zielgruppe = And("Zielgruppe", unterschicht, maennlich)
    print(zielgruppe.description())

    print(line)
    with time_log("calculating zielgruppe uplift per hour"):
        zielgruppe.calculate()
    print(zielgruppe.result.shape)

    print(line)
    zielgruppe.set_timescale("TimeSlot")
    with time_log("calculating zielgruppe uplift per time slot"):
        zielgruppe.calculate()
    print(zielgruppe.result.shape)

    print(line)
    with time_log("plotting graphics"):
        pop_heatmap = zielgruppe.plot_ch_uplift_heatmap()
        pop_barplot = zielgruppe.plot_ch_uplift_barplot()
        pop_stationmap = zielgruppe.plot_station_heatmap()
