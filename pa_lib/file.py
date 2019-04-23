#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:33:01 2019
File handling for PA data
@author: kpf
"""
import pandas as pd
import os
from datetime import datetime as dtt
from fnmatch import fnmatch

from pa_lib.const import PA_DATA_DIR
from pa_lib.log import time_log, info
from pa_lib.util import format_size


def file_size(file_path, do_format=True):
    nbytes = os.stat(file_path).st_size
    if do_format:
        return format_size(nbytes)
    else:
        return nbytes


def file_list(path='.', pattern='*.*', sort='name', desc=False, do_format=True):
    """DataFrame(name, size, mtime) for all files in path, filtered by pattern,
       sorted by 'sort' and 'desc'"""
    files = pd.DataFrame.from_records(
        columns='name size mtime'.split(),
        data=[(f.name, f.stat().st_size, dtt.fromtimestamp(f.stat().st_mtime))
              for f in os.scandir(path)
              if f.is_file() and fnmatch(f.name, pattern)])
    if sort is not None:
        files = files.sort_values(by=sort, ascending=not desc)
    if do_format:
        return files.assign(size=files['size'].apply(format_size),
                            mtime=files['mtime'].dt.strftime(
                                '%d.%m.%y %H:%M:%S'))
    else:
        return files


def data_files(pattern='[!.]*.*', sort='name', **kwargs):
    return file_list(PA_DATA_DIR, pattern, sort, **kwargs).set_index(sort)


@time_log('storing Excel')
def store_excel(df, file_name, **params):
    file_path = PA_DATA_DIR + file_name
    info('Writing to file ' + file_path)
    # if we have a default index, trash it. If it's more sophisticated, store it
    if type(df.index) == pd.RangeIndex:
        df = df.reset_index(drop=True)
    else:
        df = df.reset_index()
    df.to_excel(file_path, index=False, freeze_panes=(1, 0), **params)
    info(f'Written {file_size(file_path)}')


@time_log('storing CSV')
def store_csv(df, file_name, do_zip=True, index=False, **params):
    file_path = PA_DATA_DIR + file_name + ('.zip' if do_zip else '')
    info('Writing to file ' + file_path)
    # if we have a default index, trash it. If it's more sophisticated, store it
    if type(df.index) == pd.RangeIndex:
        df = df.reset_index(drop=True)
    else:
        df = df.reset_index()
    if do_zip:
        df.to_csv(file_path, compression='zip', index=index, **params)
    else:
        df.to_csv(file_path, index=index, **params)
    info(f'Written {file_size(file_path)}')


@time_log('loading CSV')
def load_csv(file_name, **params):
    file_path = PA_DATA_DIR + file_name
    info('Reading from file ' + file_path)
    df = pd.read_csv(file_path, low_memory=False, **params)
    return df


@time_log('storing HDF')
def store_hdf(df, file_name, **params):
    file_path = PA_DATA_DIR + file_name
    info('Writing to file ' + file_path)
    # if we have a default index, trash it. If it's more sophisticated, store it
    if type(df.index) == pd.RangeIndex:
        df = df.reset_index(drop=True)
    else:
        df = df.reset_index()
    df.to_hdf(file_path, key='df', mode='w', format='table', complevel=9,
              complib='blosc:lz4', index=False, **params)
    info(f'Written {file_size(file_path)}')


@time_log('loading HDF')
def load_hdf(file_name, **params):
    file_path = PA_DATA_DIR + file_name
    info('Reading from file ' + file_path)
    with pd.HDFStore(file_path, mode='r', **params) as ds:
        df = ds['df']
    return df


@time_log('storing binary file')
def store_bin(df, file_name, **params):
    file_path = PA_DATA_DIR + file_name
    info('Writing to file ' + file_path)
    # if we have a default index, trash it. If it's more sophisticated, store it
    if type(df.index) == pd.RangeIndex:
        df = df.reset_index(drop=True)
    else:
        df = df.reset_index()
    df.to_feather(file_path, **params)
    info(f'Written {file_size(file_path)}')


@time_log('loading binary file')
def load_bin(file_name, **params):
    file_path = PA_DATA_DIR + file_name
    info('Reading from file ' + file_path)
    df = pd.read_feather(file_path, **params)
    return df


def rm_file(file_name):
    file_path = PA_DATA_DIR + file_name
    info('Removing file ' + file_path)
    os.remove(file_path)
