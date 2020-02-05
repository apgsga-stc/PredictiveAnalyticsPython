## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Tuple

from axinova.UpliftLib import (
    VarId,
    IntList,
    StringList,
    DataFrame,
    DataSeries,
    poisson_sd,
)
from axinova.UpliftData import UpliftData
from pa_lib.util import list_items
from pa_lib.data import clean_up_categoricals


########################################################################################
# HELPER FUNCTIONS
########################################################################################
def _validate_target(target: "Target") -> None:
    assert isinstance(
        target, Target
    ), f"target of wrong type {target.__class__}, must be one of: Variable, Or, And"


def _combined_desc(kind: str, name: str, desc1: str, desc2: str, indent: str) -> str:
    description = "\n".join(
        [
            f"{name} = (",
            f"{indent}  {desc1}",
            f"{indent}{kind}",
            f"{indent}  {desc2}",
            f"{indent})",
        ]
    )
    return description


########################################################################################
# CLASSES
########################################################################################
@dataclass
class Target(ABC):
    _stations: StringList = field(init=False, default_factory=list)
    _timescale: str = field(init=False, default="Hour")
    result: DataFrame = field(init=False)
    data: UpliftData = field(init=False, default_factory=UpliftData)

    def __post_init__(self):
        self._validate()

    def __str__(self):
        return self.describe()

    def _get_counts(
        self,
        value_col: str,
        data: DataFrame,
        variable: VarId = None,
        code_labels: StringList = None,
    ) -> Tuple[DataSeries, DataSeries]:
        """Aggregate occurrence counts after filtering by selection criteria.
        Return counts and Poisson standard deviations."""
        selected_rows = data["Station"].isin(self.stations)
        if variable is not None:
            selected_rows &= data["Variable"] == variable
        if code_labels is not None:
            selected_rows &= data["Code"].isin(code_labels)
        counts = (
            data.loc[selected_rows]
            .pipe(clean_up_categoricals, incl_col="Station")
            .groupby(["Station", "DayOfWeek", self.timescale])[value_col]
            .agg("sum")
            .fillna(0)
        )
        count_sd = poisson_sd(counts)
        return counts, count_sd

    @abstractmethod
    def set_stations(self, stations: StringList) -> None:
        pass

    @property
    def stations(self) -> StringList:
        if self._stations == list():
            return self.data.all_stations
        else:
            return self._stations

    @abstractmethod
    def set_timescale(self, timescale: str) -> None:
        pass

    @property
    def timescale(self) -> str:
        return self._timescale

    @abstractmethod
    def _validate(self) -> None:
        pass

    @abstractmethod
    def calculate(self) -> None:
        pass

    @abstractmethod
    def describe(self, indent: str = "") -> str:
        pass


@dataclass
class Variable(Target):
    name: str
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
            checked_stations = self.data.all_stations
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
        ax_total_count, _ = self._get_counts(
            value_col="Value", data=self.data.ax_data, variable=self.variable
        )
        full_index = ax_total_count.index
        ax_pers_count, ax_pers_sd = self._get_counts(
            value_col="Value",
            data=self.data.ax_data,
            variable=self.variable,
            code_labels=self.code_labels,
        )
        target_ratio = (ax_pers_count / ax_total_count).fillna(0)
        target_pers = target_ratio * self._spr

        # reference ratios for CH population / all _stations / each station
        pop_ratio = self.data.ax_population_ratios(self.variable)[self.code_labels].sum(
            axis="columns"
        )[0]
        global_ratio = self.data.ax_global_ratios(self.variable)[self.code_labels].sum(
            axis="columns"
        )[0]
        station_ratios = (
            self.data.ax_station_ratios(self.variable)
            .loc[self._stations, self.code_labels]
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
        self.result = result

    def describe(self, indent: str = "") -> str:
        description = (
            f"{self.name}: '{self.var_label}' IN ['"
            + "', '".join(self.code_labels)
            + "']"
        )
        return description


@dataclass
class TargetCombination(Target, ABC):
    name: str
    target1: Target
    target2: Target

    def _validate(self) -> None:
        _validate_target(self.target1)
        _validate_target(self.target2)

    def set_stations(self, stations: StringList) -> None:
        self.target1.set_stations(stations)
        self.target2.set_stations(stations)

    def set_timescale(self, timescale: str) -> None:
        self.target1.set_timescale(timescale)
        self.target2.set_timescale(timescale)

    def calculate(self):
        self.target1.calculate()
        self.target2.calculate()


@dataclass
class And(TargetCombination):
    def calculate(self):
        TargetCombination.calculate(self)

    def describe(self, indent: str = "") -> str:
        desc1 = self.target1.describe(f"{indent}  ")
        desc2 = self.target2.describe(f"{indent}  ")
        description = _combined_desc("AND", self.name, desc1, desc2, indent)
        return description


@dataclass
class Or(TargetCombination):
    def calculate(self) -> None:
        TargetCombination.calculate(self)

    def describe(self, indent: str = "") -> str:
        desc1 = self.target1.describe(f"{indent}  ")
        desc2 = self.target2.describe(f"{indent}  ")
        description = _combined_desc("OR", self.name, desc1, desc2, indent)
        return description


########################################################################################
# TESTING CODE
########################################################################################
if __name__ == "__main__":
    line = "-" * 88
    print(line)
    kein_auto = Variable(name="Kein Auto", variable="g_220", code_nr=[0])
    wenig_einkommen = Variable(name="Wenig Einkommen", variable="md_ek", code_nr=[0, 1])
    maennlich = Variable(name="Männlich", variable="md_sex", code_nr=[0])
    unterschicht = Or("Unterschicht", target1=kein_auto, target2=wenig_einkommen)
    zielgruppe = And("Zielgruppe", target1=unterschicht, target2=maennlich)
    print(zielgruppe.describe())
