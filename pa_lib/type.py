#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 12:01:34 2019

@author: kpf
"""

import pandas as pd
from datetime import datetime as dtt
from dataclasses import dataclass, astuple
from typing import List, Any, Iterator

from pa_lib.util import format_size


########################################################################################################################
# TYPES
########################################################################################################################
dtFactor = pd.api.types.CategoricalDtype(ordered=True)
dtYear = pd.api.types.CategoricalDtype(ordered=True)
dtKW = pd.api.types.CategoricalDtype(
    categories=[w + 1 for w in range(53)], ordered=True
)
dtDayOfWeek = pd.api.types.CategoricalDtype(
    categories="Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split(),
    ordered=True,
)


def cat_ordered_type(categories: List[str]) -> pd.api.types.CategoricalDtype:
    return pd.api.types.CategoricalDtype(categories=categories, ordered=True)


########################################################################################################################
# CLASSES
########################################################################################################################
@dataclass
class ConnectPar:
    """Holds Oracle Connection parameters"""

    instance: str
    user: str
    passwd: str

    def astuple(self) -> tuple:
        return astuple(self)


@dataclass
class File:
    name: str
    size: int
    mtime: dtt

    def __str__(self):
        return f'"{self.name}" ({format_size(self.size)}), last modified {dtt.strftime(self.mtime, "%Y-%m-%d %H:%M:%S")}'


class Record(object):
    """
    Simple container class, allows both attribute and dict item access (.item and ["item"])
    """

    def __init__(self, **kwargs):
        """Initialize from keyword parameters"""
        for (key, value) in kwargs.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        attr_repr = ", ".join(f"{k}={repr(v)}" for (k, v) in self.items())
        return f"Record({attr_repr})"

    def __getitem__(self, key: str) -> Any:
        """Get attribute like from a dict"""
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set attribute like on a dict"""
        setattr(self, key, value)

    def __iter__(self) -> Iterator:
        return iter(self.__dict__)

    def __len__(self) -> int:
        return len(self.__dict__)

    def keys(self) -> list:
        return list(self.__dict__.keys())

    def values(self) -> list:
        return list(self.__dict__.values())

    def items(self) -> list:
        return list(self.__dict__.items())


########################################################################################################################
# HELPER FUNCTIONS
########################################################################################################################
def as_years(col: pd.Series) -> pd.Series:
    """
    Convert Series to Year type

    :param col: Series to convert
    :return: converted Series
    """
    return col.astype("int").astype(dtYear)


def as_kw(col: pd.Series) -> pd.Series:
    """
    Convert Series to calendar week type

    :param col: Series to convert
    :return: converted Series
    """
    return col.astype("int").astype(dtKW)


def merge_years(col1: pd.Series, col2: pd.Series) -> pd.Series:
    """
    Merge two dataframe columns containing year numbers. Result will be ordered correctly.

    :param col1: First column
    :param col2: Second column
    :return: merged column
    """
    merged = pd.api.types.union_categoricals(
        to_union=[col1.astype(dtYear), col2.astype(dtYear)],
        ignore_order=True,
        sort_categories=True,
    )
    return merged.as_ordered()


if __name__ == "__main__":
    test = Record(a=1, b=2, c="abc", d=[1, 2, 3], e=Record(e1="a", e2="b"))
    print(test)
