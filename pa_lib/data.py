#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:33:01 2019
Data frame related functions
@author: kpf
"""
import numpy as np
import pandas as pd

from pa_lib.type import dtFactor, dtKW, dtYear
from pa_lib.util import format_size, flatten, flat_list


########################################################################################
def desc_col(df, det=False):
    if det:
        return df.apply(
            lambda col: pd.Series(
                [
                    col.dtype,
                    f"{len(col) - col.count()}/{col.count()}",
                    col.nunique(),
                    format_size(col.memory_usage(deep=True)),
                    f"[{col.min(skipna=True)},{col.max(skipna=True)}]",
                ],
                index="DTYPE NULLS UNIQUE MEM RANGE".split(),
            ),
            result_type="expand",
        ).transpose()
    return df.apply(
        lambda col: pd.Series(
            [col.dtype, f"{len(col) - col.count()}/{col.count()}", col.nunique()],
            index="DTYPE NULLS UNIQUE".split(),
        ),
        result_type="expand",
    ).transpose()


def flatten_multi_index_cols(df, sep="_"):
    df_flattened = df.copy()
    if isinstance(df_flattened.columns, pd.MultiIndex):
        col_names = list(map(sep.join, df_flattened.columns.to_flat_index()))
        df_flattened.set_axis(col_names, axis="columns", inplace=True)
    return df_flattened


########################################################################################
def select_columns(df, incl_col=None, incl_pattern=None, incl_dtype=None):
    """Filter column list. Specify column names, patterns, or source dtypes"""
    col_list = list()
    if incl_col is not None:
        col_list.extend([c for c in flatten(incl_col) if c in df.columns])
    if incl_pattern is not None:
        flat_df = flatten_multi_index_cols(df, sep="|")
        for pat in flatten(incl_pattern):
            col_list.extend(flat_df.columns[flat_df.columns.str.match(pat)])
    if incl_dtype is not None:
        col_list.extend(df.select_dtypes(include=incl_dtype).columns)
    return list(set(col_list))  # make unique


def as_dtype(df, to_dtype, **selectors):
    """Convert columns to a target dtype.
       **selectors incl_col=None, incl_pattern=None, incl_dtype
       are passed to select_columns()"""
    res = df.copy()
    for col in select_columns(df, **selectors):
        res.loc[:, col] = df.loc[:, col].astype(to_dtype)
    return res


def as_date(df, format_str, **selectors):
    """Convert columns to datetime64, using 'format_str'.
       **selectors incl_col=None, incl_pattern=None, incl_dtype
       are passed to select_columns()"""
    for col in select_columns(df, **selectors):
        df.loc[:, col] = pd.to_datetime(df.loc[:, col], format=format_str)
    return df


def as_int_factor(df, **selectors):
    """Convert columns to Categorial of integers, keeping NaNs.
       **selectors incl_col=None, incl_pattern=None, incl_dtype
       are passed to select_columns()"""
    col_list = select_columns(df, **selectors)
    df[col_list] = (
        df[col_list].astype("float").astype(pd.CategoricalDtype(ordered=True))
    )
    for col in col_list:
        df[col] = df[col].cat.set_categories(
            df[col].cat.categories.astype("int"), ordered=True
        )
    return df


def unfactorize(df):
    """Convert categorical columns to their categories' data type"""
    df = df.copy()
    for col in select_columns(df, incl_dtype="category"):
        nulls = df[col].isna()
        df[col] = df[col].astype(df[col].cat.categories.dtype)
        # Integer columns with Nulls need fixing, will convert to float
        if any(nulls):
            df.loc[nulls, col] = np.NaN
    return df


def as_kw(df, **selectors):
    """Convert columns to KW type.
       **selectors incl_col=None, incl_pattern=None, incl_dtype
       are passed to select_columns()"""
    col_list = select_columns(df, **selectors)
    df[col_list] = df[col_list].astype(dtFactor)
    for col in col_list:
        df[col] = df[col].cat.set_categories(dtKW.categories)
    return df


def clean_up_categoricals(df, **selectors):
    """Drop unused categories on Categoricals of a DataFrame
       **selectors incl_col=None, incl_pattern=None, incl_dtype
       are passed to select_columns()"""
    if len(selectors) > 0:
        col_list = select_columns(df, **selectors)
    else:
        col_list = select_columns(df, incl_dtype="category")
    for col in col_list:
        df[col].cat.remove_unused_categories(inplace=True)
    return df


########################################################################################
def select_rows(df: pd.DataFrame, selectors: dict) -> pd.DataFrame:
    """Return dataframe df filtered by selectors {column/index level -> value(s)}.
       Inspired by SQL Select.
       Column selectors are ANDed, values for one column ORed."""
    row_mask = pd.Series([True] * df.shape[0])
    try:
        source_df = df.reset_index()[list(selectors.keys())]
    except KeyError:
        raise ValueError(
            f"Bad selector columns {list(selectors.keys())}, "
            + f"dataframe has {df.reset_index().columns.to_list()}"
        ) from None
    for col, values in selectors.items():
        row_mask &= source_df[col].isin(flat_list(values))
    result = (
        df.loc[row_mask.values]
        .pipe(clean_up_categoricals)
        .pipe(as_dtype, dtFactor, incl_dtype="object")
    )
    return result


########################################################################################
def replace_col(df, col, with_col, where):
    """Replace values of columns 'col' with values from columns 'with_col'
       where 'where' is True"""
    res = df.copy()
    row_mask = df.eval(where)
    res.loc[row_mask, col] = res.loc[row_mask, with_col]
    return res


def fillna_col(df, col, with_col):
    """Replace values of columns 'col' with values from columns 'with_col'
       where col is NA"""
    res = df.copy().fillna({col: df[with_col]})
    return res


def cond_col(df, col, cond, true_col, else_col=None):
    """Make new column 'col' out of 'true_col' or 'else_col',
       depending on 'cond'"""
    row_mask = df.eval(cond)
    df = df.assign(**{col: np.nan})
    df.loc[row_mask, col] = df.loc[row_mask, true_col]
    if else_col is not None:
        df.loc[~row_mask, col] = df.loc[~row_mask, else_col]
    return df


def calc_col_partitioned(df, col, fun, on, part_by):
    """Create new column(s) 'col' in df by applying 'fun' to column(s) 'on'
       partitioned by 'part_by'"""
    new_col = df.groupby(part_by, observed=True)[flat_list(on)].transform(fun)
    new_col_names = flat_list(col)
    return df.assign(**new_col.set_axis(new_col_names, axis="columns", inplace=False))


def make_sumcurve(df, sum_col_name, crv_col_name, on, part_by):
    """create a sumcurve (and a sum) column on 'on', partitioned by 'part_by'
       Order of entries will be as in the original df"""
    df = (
        df.pipe(calc_col_partitioned, sum_col_name, fun="sum", on=on, part_by=part_by)
        .pipe(calc_col_partitioned, crv_col_name, fun="cumsum", on=on, part_by=part_by)
        .eval(f"{crv_col_name} = {crv_col_name} / {sum_col_name}")
    )
    return df


########################################################################################
def make_isoyear(df, dt_col, yr_col="YEAR"):
    df.loc[:, yr_col] = df[dt_col].dt.strftime("%G").astype("int16")
    return df


def make_isoweek(df, dt_col, kw_col="KW"):
    df.loc[:, kw_col] = df[dt_col].dt.strftime("%V").astype("int8")
    return df


def split_date_iso(df, dt_col, yr_col="YEAR", kw_col="KW"):
    """Split a date column into ISO year and ISO week as new columns"""

    def isoyear_isokw(date):
        return date.isocalendar()[:-1]

    not_null = df[dt_col].notna()
    year_kw = pd.DataFrame.from_records(
        columns=[yr_col, kw_col],
        data=(df.loc[not_null, dt_col].dt.date.apply(isoyear_isokw).to_list()),
        index=df.index[not_null],
    ).astype({yr_col: dtYear, kw_col: dtKW})
    df = df.assign(**dict(year_kw))
    return df


def make_isoweek_rd(df, kw_col, round_by=()):
    """Round a KW column to {round_by}-week periods.
       New columns are named {kw_col}_{round_by}"""
    kw = df.loc[df[kw_col].notna(), kw_col].astype("int8")
    roundings = [(rd, f"{kw_col}_{rd}") for rd in flatten(round_by)]
    for (rd, rd_col) in roundings:
        df = df.assign(**{rd_col: kw - (kw - 1) % rd})
        # round any week 53 to last complete period
        if rd in (2, 4):
            df.loc[df[rd_col] == 53, rd_col] = 53 - rd
        df = as_kw(df, incl_col=rd_col)
    return df


def make_period_diff(
    df, year_col_1, period_col_1, year_col_2, period_col_2, diff_col="diff", round_by=2
):
    """Calculates difference (in periods) between two year/period column pairs"""
    return df.eval(
        f"{diff_col} = ({year_col_2} - {year_col_1}) * (52 // {round_by}) \
                                + ({period_col_2} - {period_col_1})"
    )


########################################################################################
def lookup(target_df, target_col_name, match_col, target_match_col_name=None):
    """Create new column from target_df["target_col"], by matching match_col with 
       target_df["target_match_col_name"].
       If target_match_col_name is not specified, match_col.name is used.
    """
    if target_match_col_name is None:
        target_match_col_name = match_col.name

    lookup_df = target_df.set_index(target_match_col_name)
    new_col = (
        lookup_df.loc[~lookup_df.index.duplicated(), target_col_name]
        .reindex(match_col)
        .values
    )
    return new_col


########################################################################################
def merge_categories(series, cat, into_cat):
    """On a categorical series, merge all instances of cat into into_cat and remove cat from categories"""
    series[series == cat] = into_cat
    return series.cat.remove_unused_categories()


def cut_categorical(series, left_limits, labels=None, cat_convert=lambda x: x):
    """Cuts an ordered categorical series into bins specified by 'left_limits' and named as 'labels' 
       (defaults to 'left_limits').
       'cat_convert' allows to specify 'left_limits' in terms of a function on the series' categories
       (defaults to identity).
    """
    if not pd.api.types.is_categorical_dtype(series) or not series.cat.ordered:
        raise TypeError("Series must be an ordered categorical")
    if labels is None:
        labels = left_limits
    elif len(labels) != len(left_limits):
        raise ValueError("'labels' and 'left_limits' must have the same length")

    # pd.cut works on numeric columns only. So, we cut the codes of our categorical
    codes = series.cat.codes

    # apply cat_convert to our categories
    categories = map(cat_convert, series.cat.categories)

    # get bins by mapping left_limits to corresponding codes, adding an upper maximum limit
    limit_code = {category: code for code, category in enumerate(categories)}
    bins = list(map(lambda limit: limit_code[limit], left_limits)) + [
        max(limit_code.values()) + 1
    ]

    return pd.cut(codes, bins=bins, labels=labels, right=False)


########################################################################################
def chi2_expected(matrix):
    """Calculates expected cell values for a contingency matrix"""
    row_sums = np.sum(matrix, axis=1)
    col_sums = np.sum(matrix, axis=0)
    total = np.sum(row_sums)
    expected = np.outer(row_sums, col_sums) / total
    if type(matrix) == pd.DataFrame:
        return pd.DataFrame(data=expected, index=matrix.index, columns=matrix.columns)
    else:
        return expected
