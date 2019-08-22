#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:33:01 2019
File handling for PA data
@author: kpf
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime as dtt
from string import ascii_uppercase as alphabet
from functools import partial

from pa_lib.const import PA_DATA_DIR
from pa_lib.log import time_log, info
from pa_lib.util import format_size
from pa_lib.data import flatten_multi_index_cols

flatten_multi_cols = partial(flatten_multi_index_cols, sep='|')

def _check_index(df):
    # if we have a default index, trash it. If it's more sophisticated, store it
    if type(df.index) == pd.RangeIndex:
        df = df.reset_index(drop=True)
    else:
        df = df.reset_index()
    return df

    
def file_size(file_path, do_format=True):
    nbytes = Path(file_path).stat().st_size
    if do_format:
        return format_size(nbytes)
    else:
        return nbytes


def file_list(path='.', pattern='*.*', sort='name', desc=False, do_format=True):
    """DataFrame(name, size, mtime) for all files in path, filtered by pattern,
       sorted by 'sort' and 'desc'"""
    files = pd.DataFrame.from_records(
        columns='name size mtime'.split(),
        data=[(name, stat.st_size, dtt.fromtimestamp(stat.st_mtime))
              for (name, stat) in ((f.name, f.stat()) for f in Path(path).iterdir()
                                   if f.is_file() and f.match(pattern))])
    if sort is not None:
        files = files.sort_values(by=sort, ascending=not desc)
    if do_format and files.shape[0] > 0:
        return files.assign(size=files['size'].apply(format_size),
                            mtime=files['mtime'].dt.strftime(
                                '%d.%m.%y %H:%M:%S'))
    else:
        return files


def data_files(pattern='[!.]*.*', sort='name', **kwargs):
    return file_list(PA_DATA_DIR, pattern, sort, **kwargs).set_index(sort)


@time_log('storing Excel')
def store_excel(df, file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Writing to file {file_path}')
    df = (df.pipe(_check_index)
            .pipe(flatten_multi_cols))
    df.to_excel(file_path, index=False, freeze_panes=(1, 0), **params)
    info(f'Written {file_size(file_path)}')


@time_log('storing CSV')
def store_csv(df, file_name, do_zip=True, index=False, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    if do_zip:
        file_path = file_path.with_name(file_path.name + '.zip')
    info(f'Writing to file {file_path}')
    df = (df.pipe(_check_index)
            .pipe(flatten_multi_cols))
    if do_zip:
        df.to_csv(file_path, compression='zip', index=index, **params)
    else:
        df.to_csv(file_path, index=index, **params)
    info(f'Written {file_size(file_path)}')


@time_log('loading CSV')
def load_csv(file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Reading from file {file_path}')
    df = pd.read_csv(file_path, low_memory=False, **params)
    return df


@time_log('storing HDF')
def store_hdf(df, file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Writing to file {file_path}')
    df = (df.pipe(_check_index)
            .pipe(flatten_multi_cols))
    df.to_hdf(file_path, key='df', mode='w', format='table', complevel=9,
              complib='blosc:lz4', index=False, **params)
    info(f'Written {file_size(file_path)}')


@time_log('loading HDF')
def load_hdf(file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Reading from file {file_path}')
    with pd.HDFStore(file_path, mode='r', **params) as ds:
        df = ds['df']
    return df


@time_log('storing binary file')
def store_bin(df, file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Writing to file {file_path}')
    df = (df.pipe(_check_index)
            .pipe(flatten_multi_cols))
    df.to_feather(file_path, **params)
    info(f'Written {file_size(file_path)}')


@time_log('loading binary file')
def load_bin(file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Reading from file {file_path}')
    df = pd.read_feather(file_path, **params)
    return df


def rm_file(file_name):
    file_path = Path(file_name).resolve()
    info(f'Removing file {file_path}')
    file_path.unlink()


def rm_data_file(file_name):
    file_path = PA_DATA_DIR / file_name
    rm_file(file_path)

    
@time_log('writing xlsx file')
def write_xlsx(df, file_name, sheet_name='df'):
    """Write df into a XLSX with fixed title row, enable auto filters"""
    df = (df.pipe(_check_index)
            .pipe(flatten_multi_cols))

    # column widths as max strlength of column's contents, or strlength of column's header if greater
    col_width = np.maximum(df.astype('str').apply(lambda col: max(col.str.len())).to_list(), 
                           list(map(len, df.columns)))
    file_path = PA_DATA_DIR / file_name
    info(f'Writing to file {file_path}')
    
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name=sheet_name)
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    
    ncols = df.shape[-1]
    title_cells = (0, 0, 0, ncols-1)
    bold = workbook.add_format({'bold': True, 'align': 'left'})
    worksheet.set_row(0, cell_format=bold)
    worksheet.autofilter(*title_cells)
    worksheet.freeze_panes(1, 0)
    # Column autowidth
    for col in range(ncols):
        worksheet.set_column(col, col, col_width[col] + 1)
    
    writer.save()
    info(f'Written {file_size(file_path)}')


@time_log('loading xlsx file')
def load_xlsx(file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Reading from file {file_path}')
    df = pd.read_excel(file_path, **params)
    return df

    
def store_pickle(df, file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Writing to file {file_path}')
    df.to_pickle(file_path, **params)
    info(f'Written {file_size(file_path)}')

    
@time_log('loading pickle file')
def load_pickle(file_name, **params):
    file_path = (PA_DATA_DIR / file_name).resolve()
    info(f'Reading from file {file_path}')
    df = pd.read_pickle(file_path, **params)
    return df
