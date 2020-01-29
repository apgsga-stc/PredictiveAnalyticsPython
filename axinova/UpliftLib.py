import pandas as pd
from typing import List, Dict, Tuple
from dataclasses import dataclass

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
# Global Objects
########################################################################################
all_weekdays = "Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split()


########################################################################################
# Helper Functions
########################################################################################
def poisson_sd(data: DataSeries) -> DataSeries:
    """Return poisson standard deviations for a series of counts => sqrt(count)."""
    return data.pow(0.5)


def combine_sd_ratios(data1: DataSeries, data2: DataSeries) -> DataSeries:
    """Aggregate standard deviations of two independent var => sqrt(sd1^2 + sd2^2)."""
    return (data1.pow(2) + data2.pow(2)).pow(0.5)


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
