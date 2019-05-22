#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:33:01 2019
Data frame related functions
@author: kpf
"""
import pandas as pd
import numpy as np

from pa_lib.util import format_size, flatten
from pa_lib.types import dtFactor, dtKW, dtYear


def desc_col(df, det=False):
    if det:
        return df.apply(
            lambda col: pd.Series(
                [col.dtype, f'{len(col) - col.count()}/{col.count()}',
                 col.nunique(),
                 format_size(col.memory_usage(deep=True)),
                 f'[{col.min(skipna=True)},{col.max(skipna=True)}]'],
                index='DTYPE NULLS UNIQUE MEM RANGE'.split()),
            result_type='expand').transpose()
    else:
        return df.apply(
            lambda col: pd.Series(
                [col.dtype, f'{len(col) - col.count()}/{col.count()}',
                 col.nunique()],
                index='DTYPE NULLS UNIQUE'.split()),
            result_type='expand').transpose()


def select_columns(df, incl_col=None, incl_pattern=None, incl_dtype=None):
    """Filter column list. Specify source dtypes, column names, or patterns"""
    col_list = list()
    if incl_col is not None:
        col_list.extend([c for c in flatten(incl_col) if c in df.columns])
    if incl_pattern is not None:
        for pat in flatten(incl_pattern):
            col_list.extend(df.columns[df.columns.str.match(pat)])
    if incl_dtype is not None:
        col_list.extend(df.select_dtypes(include=incl_dtype).columns)
    return list(set(col_list))  # make unique


def as_dtype(df, to_dtype, **selectors):
    """Convert columns to a target dtype.
       **selectors are passed to select_columns()"""
    for col in select_columns(df, **selectors):
        df.loc[:, col] = df.loc[:, col].astype(to_dtype)
    return df


def as_date(df, format_str, **selectors):
    """Convert columns to datetime64, using 'format_str'.
       **selectors are passed to select_columns()"""
    for col in select_columns(df, **selectors):
        df.loc[:, col] = pd.to_datetime(df.loc[:, col], format=format_str)
    return df


def as_int_factor(df, **selectors):
    """Convert columns to Categorial of integers, keeping NaNs.
       **selectors are passed to select_columns()"""
    col_list = select_columns(df, **selectors)
    df[col_list] = df[col_list].astype('float').astype(
        pd.CategoricalDtype(ordered=True))
    for col in col_list:
        df[col] = df[col].cat.set_categories(
            df[col].cat.categories.astype('int'), ordered=True)
    return df


def as_kw(df, **selectors):
    """Convert columns to KW type. **selectors are passed to select_columns()"""
    col_list = select_columns(df, **selectors)
    df[col_list] = df[col_list].astype(dtFactor)
    for col in col_list:
        df[col] = df[col].cat.set_categories(dtKW.categories)
    return df


def clean_up_categoricals(df, **selectors):
    """Drop unused categories on Categoricals of a DataFrame"""
    if len(selectors) > 0:
        col_list = select_columns(df, **selectors)
    else:
        col_list = select_columns(df, incl_dtype='category')
    for col in col_list:
        df[col].cat.remove_unused_categories(inplace=True)
    return df


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
    """Create new column 'col' in df by applying 'fun' to 'on'
       partitioned by 'part_by'"""
    new_col = df.groupby(part_by, observed=True)[on].transform(fun)
    return df.assign(**{col: new_col})


def make_sumcurve(df, sum_col_name, crv_col_name, on, part_by):
    """create a sumcurve (and a sum) column on 'on', partitioned by 'part_by'
       Order of entries will be as in the original df"""
    df = (df
          .pipe(calc_col_partitioned, sum_col_name, fun='sum', on=on,
                part_by=part_by)
          .pipe(calc_col_partitioned, crv_col_name, fun='cumsum', on=on,
                part_by=part_by)
          .eval(f'{crv_col_name} = {crv_col_name} / {sum_col_name}'))
    return df


def make_isoyear(df, dt_col, yr_col='YEAR'):
    df.loc[:, yr_col] = df[dt_col].dt.strftime('%G').astype('int16')
    return df


def make_isoweek(df, dt_col, kw_col='KW'):
    df.loc[:, kw_col] = df[dt_col].dt.strftime('%V').astype('int8')
    return df


def make_isoweek_rd(df, kw_col, round_by=()):
    """Round a KW column to {round_by}-week periods.
       New columns are named {kw_col}_{round_by}"""
    kw = df.loc[df[kw_col].notna(), kw_col].astype('int8')
    roundings = [(rd, f'{kw_col}_{rd}') for rd in flatten(round_by)]
    for (rd, rd_col) in roundings:
        df = df.assign(**{rd_col: kw - (kw - 1) % rd})
        # round any week 53 to last complete period
        if rd in (2, 4):
            df.loc[df[rd_col] == 53, rd_col] = 53 - rd
        df = as_kw(df, incl_col=rd_col)
    return df


def split_date_iso(df, dt_col, yr_col='YEAR', kw_col='KW'):
    """Split a date column into ISO year and ISO week as new columns"""

    def isoyear_isokw(date):
        return date.isocalendar()[:-1]

    not_null = df[dt_col].notna()
    year_kw = pd.DataFrame.from_records(
        columns=[yr_col, kw_col],
        data=(df.loc[not_null, dt_col]
              .dt.date.apply(isoyear_isokw)
              .to_list()),
        index=df.index[not_null]
    ).astype({yr_col: dtYear, kw_col: dtKW})
    df = df.assign(**dict(year_kw))
    return df
