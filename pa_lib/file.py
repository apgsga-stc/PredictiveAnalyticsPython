#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:33:01 2019
File handling for PA data
@author: kpf
"""
import pandas as pd
import numpy as np
import re
import sys
import pickle
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime as dtt
from functools import partial
from typing import Union, Any

from pa_lib.const import PA_DATA_DIR
from pa_lib.log import time_log, info
from pa_lib.util import format_size
from pa_lib.data import flatten_multi_index_cols, as_dtype, clean_up_categoricals
from pa_lib.type import dtFactor

flatten_multi_cols = partial(flatten_multi_index_cols, sep="|")

# global state: sub-directory of PA_DATA_DIR we're working in
_PROJECT_DIR = PA_DATA_DIR  # initialize to base data directory


def set_project_dir(dir_name: str) -> None:
    """
    Set current project dir to work in.
    If dir_name starts with "/", path is supposed to be absolute.
    Otherwise, it's interpreted to be below PA_DATA_DIR.
    Directory will be created if not existing.

    :param dir_name: directory name
    """
    global _PROJECT_DIR

    if str(dir_name)[0] == "/":
        new_project_dir = Path(dir_name).resolve()
    else:
        new_project_dir = PA_DATA_DIR / dir_name
    if not new_project_dir.exists():
        new_project_dir.mkdir()
    if new_project_dir.is_dir():
        _PROJECT_DIR = new_project_dir
    else:
        raise FileExistsError(
            f"Can't create directory '{dir_name}': File exists in {PA_DATA_DIR}"
        )


def reset_project_dir():
    """
    Reset current project dir to initial status (base data directory)
    """
    global _PROJECT_DIR
    _PROJECT_DIR = PA_DATA_DIR


def get_project_dir():
    """Currently set project dir"""
    return _PROJECT_DIR


@contextmanager
def project_dir(dir_name):
    """
    Temporarily switch into another project dir

    :param dir_name: name of project directory to sitch into
    """
    global _PROJECT_DIR
    previous_project_dir = _PROJECT_DIR
    set_project_dir(dir_name)
    try:
        yield
    finally:
        _PROJECT_DIR = previous_project_dir


########################################################################################
def file_list(
    path: str = ".",
    pattern: str = "*.*",
    sort: str = "name",
    desc: bool = False,
    do_format: bool = True,
) -> pd.DataFrame:
    """
    List of files in a directory

    :param path: directory name to list files from
    :param pattern: Shell glob pattern to filter files by
    :param sort: column to sort by (one of name, size, mtime)
    :param desc: sort descending?
    :param do_format: return values in human-friendly format?
    :return: file list as a DataFrame
    """
    files = pd.DataFrame.from_records(
        columns="name size mtime".split(),
        data=[
            (name, stat.st_size, dtt.fromtimestamp(stat.st_mtime))
            for (name, stat) in (
                (f.name, f.stat())
                for f in Path(path).iterdir()
                if f.is_file() and f.match(pattern)
            )
        ],
    )
    if sort is not None:
        files = files.sort_values(by=sort, ascending=not desc)
    if do_format and files.shape[0] > 0:
        return files.assign(
            size=files["size"].apply(format_size),
            mtime=files["mtime"].dt.strftime("%d.%m.%y %H:%M:%S"),
        )
    else:
        return files


def data_files(pattern: str = "[!.]*.*", sort: str = "name", **kwargs) -> pd.DataFrame:
    """
    Return all files in current project dir. Wraps `file_list()`

    :param pattern: Shell glob pattern to filter by. By default, hide invisibles
    :param sort: column to sort by
    :param kwargs: other arguments, passed to `file_list`
    :return: file list as a DataFrame
    """
    return file_list(_PROJECT_DIR, pattern, sort, **kwargs).set_index(sort)


########################################################################################
def file_size(file_path: str, do_format: bool = True) -> Union[str, int]:
    """
    Return size of a stored file

    :param file_path: file to examine
    :param do_format: Format size for humans to read (in MB, KB, B)
    :return: text with formatted size (if `do_format`) or number of bytes
    """
    nbytes = Path(file_path).stat().st_size
    if do_format:
        return format_size(nbytes)
    else:
        return nbytes


########################################################################################
@time_log("storing text file")
def store_txt(txt: str, file_name: str, **params) -> None:
    """
    Store string into a text file in current project directory

    :param txt: string to be stored
    :param file_name: name of text file
    :param params: will be passed to `file.open()`
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    info(f"Writing to file {file_path}")
    with file_path.open("w", **params) as file:
        file.write(txt)
    info(f"Written {file_size(file_path)}")


@time_log("loading text file")
def load_txt(file_name: str, **params: Any) -> str:
    """
    Load text file from current project dir into string variable

    :param file_name: name of file to load
    :param params: will be passed to `file.open()`
    :return: full text contents of file
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    info(f"Reading from file {file_path}")
    with file_path.open("r", **params) as file:
        txt = file.read()
    return txt


########################################################################################
def rm_file(file_path: str) -> None:
    """
    Remove file
    :param file_path: file to remove
    """
    file_path = Path(file_path).resolve()
    info(f"Removing file {file_path}")
    file_path.unlink()


def rm_data_file(file_name: str) -> None:
    """
    Remove a file from the current project directory. Wraps `rm_file()`

    :param file_name: file to remove
    """
    file_path = _PROJECT_DIR / file_name
    rm_file(file_path)


def make_file_name(file_name: str) -> str:
    """
    Clean up file name (remove non-alphanumerics and space runs)

    :param file_name: file name to clean up
    """
    new_name = re.sub(r" +", " ", re.sub(r"\W", " ", file_name))
    return new_name


########################################################################################
def optimize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize a data frame for storage:
     * Convert string columns to categoricals
     * Drop index
     * Drop unused categories of all categoricals

    :param df: DataFrame to clean up
    :return: cleaned DataFrame
    """
    clean_df = df.copy()
    clean_df = (
        clean_df.pipe(as_dtype, dtFactor, incl_dtype="object")
        .reset_index(drop=True)
        .pipe(clean_up_categoricals)
    )
    return clean_df


########################################################################################
def _check_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Un-index a DataFrame:
     * If we have a default index, trash it
     * If it's more sophisticated, store it

    :param df: DataFrame to work on
    :return: cleaned DataFrame
    """
    do_drop = type(df.index) == pd.RangeIndex
    df_checked = df.reset_index(drop=do_drop)
    return df_checked


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make DataFrame storable as simple table:
     * remove index using `_check_index()`
     * turn hierarchical columns into simple columns using `data.flatten_multi_cols()`
    :param df: DataFrame to normalize
    :return: normalized DataFrame
    """
    return df.pipe(_check_index).pipe(flatten_multi_cols)


def _store_df(df: pd.DataFrame, file_name: str, file_type: str, **params: Any) -> None:
    """
    Internal function to store a dataframe to the current project directory as one of several file types

    :param df: DataFrame to store
    :param file_name: File name to use
    :param file_type: file type to store as
    :param params: will be passed to the type-specific implementation
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    df_out = pd.DataFrame()

    # Pickle files support all python objects. For other formats, assume a DataFrame and normalize it.
    if file_type == "xlsx":
        pass
    elif file_type == "pickle":
        df_out = df
    else:
        df_out = _normalize(df)

    # Back up target file, if it already exists
    time_stamp = dtt.now().strftime("%Y%m%d-%H%M%S-%f")
    bkup_path = file_path.with_name(f"{time_stamp}_{file_name}")
    if file_path.exists():
        file_path.replace(bkup_path)

    with time_log(f"storing {file_type} file"):
        info(f"Writing to file {file_path}")
        try:
            if file_type == "feather":
                df_out.to_feather(file_path, **params)
            elif file_type == "csv":
                compression = params.pop("compression", None)
                do_zip = params.pop("do_zip", False)
                if do_zip:
                    compression = "zip"
                index = params.pop("index", False)
                df_out.to_csv(file_path, compression=compression, index=index, **params)
            elif file_type == "pickle":
                with open(file_path, "wb") as file:
                    pickle.dump(df_out, file, protocol=pickle.HIGHEST_PROTOCOL)
            elif file_type == "hdf":
                key = params.pop("key", "df")
                index = params.pop("index", False)
                df_out.to_hdf(
                    file_path,
                    key=key,
                    mode="w",
                    format="table",
                    complevel=9,
                    complib="blosc:lz4",
                    index=index,
                    **params,
                )
            elif file_type == "xlsx":
                sheets = params.pop("sheets", {"df": df})
                index = params.pop("index", False)
                file_path = _PROJECT_DIR / file_name
                writer = pd.ExcelWriter(file_path, engine="xlsxwriter")
                workbook = writer.book
                bold = workbook.add_format({"bold": True, "align": "left"})
                for sheet_name, df in sheets.items():
                    df_out = _normalize(df)
                    df_out.to_excel(writer, index=index, sheet_name=sheet_name)
                    # Formatting
                    worksheet = writer.sheets[sheet_name]
                    ncols = df_out.shape[-1]
                    title_cells = (0, 0, 0, ncols - 1)
                    worksheet.set_row(0, cell_format=bold)
                    worksheet.autofilter(*title_cells)
                    worksheet.freeze_panes(1, 0)
                    # Column autowidth
                    # column widths as max strlength of column's contents,
                    # or strlength of column's header if greater
                    col_width = np.maximum(
                        df_out.astype("str")
                        .apply(lambda column: max(column.str.len()))
                        .to_list(),
                        list(map(len, df_out.columns)),
                    )
                    for col in range(ncols):
                        worksheet.set_column(col, col, col_width[col] + 1)
                writer.save()
            else:
                raise ValueError(f"Unknown file type '{file_type}'")
        except:
            if file_path.exists():
                file_path.unlink()
            if bkup_path.exists():
                bkup_path.replace(file_path)
            raise IOError(f"Failed writing file {file_path}: {sys.exc_info()[1]}")
        else:
            if bkup_path.exists():
                bkup_path.unlink()
            info(f"Written {file_size(file_path)}")


def store_bin(df: pd.DataFrame, file_name: str, **params: Any) -> None:
    """
    Store df as a **.feather** file in current project directory. Wraps `_store_df()`

    :param df: DataFrame to store
    :param file_name: file name to use
    :param params: will be passed to `df.to_feather()`
    """
    _store_df(df, file_name, file_type="feather", **params)


@time_log("loading binary file")
def load_bin(file_name: str, **params: Any) -> pd.DataFrame:
    """
    Load data from a **.feather** file into a DataFrame

    :param file_name: file to load
    :param params: will be passed to `pd.read_feather()`
    :return: data from file
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    info(f"Reading from file {file_path}")
    df = pd.read_feather(file_path, **params)
    return df


def store_pickle(df, file_name):
    """
    Store df as a **pickle** file in current project directory. Wraps `_store_df()`

    :param df: DataFrame to store
    :param file_name: file name to use
    """
    _store_df(df, file_name, file_type="pickle")


@time_log("loading pickle file")
def load_pickle(file_name: str) -> Any:
    """
    Load data from a **pickle** file into a Python object

    :param file_name: file to load
    :return: data from file
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    info(f"Reading from file {file_path}")
    with open(file_path, "rb") as file:
        obj = pickle.load(file)
    return obj


def store_csv(df, file_name, **params):
    """
    Store df as a **.csv** file in current project directory. Wraps `_store_df()`

    :param df: DataFrame to store
    :param file_name: file name to use
    :param params: will be passed to `df.to_csv()`, e.g. compression="zip"
    """
    _store_df(df, file_name, file_type="csv", **params)


@time_log("loading CSV file")
def load_csv(file_name, **params):
    """
    Load data from a **.csv** file into a DataFrame

    :param file_name: file to load
    :param params: will be passed to `pd.read_csv()`
    :return: data from file
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    info(f"Reading from file {file_path}")
    df = pd.read_csv(file_path, low_memory=False, **params)
    return df


def store_hdf(df, file_name, **params):
    """
    Store df into a **.hdf** data store (element "df") in current project directory. Wraps `_store_df()`

    :param df: DataFrame to store
    :param file_name: file name to use
    :param params: will be passed to `df.to_hdf()` (e.g. key="df", index=False)
    """
    _store_df(df, file_name, file_type="hdf", **params)


@time_log("loading HDF file")
def load_hdf(file_name, **params):
    """
    Load data from a **.hdf** data store (element "df) into a DataFrame

    :param file_name: file to load
    :param params: will be passed to `pd.HDFStore()`
    :return: data from file
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    info(f"Reading from file {file_path}")
    with pd.HDFStore(file_path, mode="r", **params) as ds:
        df = ds["df"]
    return df


def store_xlsx(df, file_name, **params):
    """
    Store df as a **.xlsx** file in current project directory. Wraps `_store_df()`
    Several dataframes can be stored as individual sheets by passing them in a "sheets" dict.
    In that case, parameter "df" is ignored.
    All sheets are formatted:
     * title row bold
     * title row frozen
     * auto filters on title row active
     * column width optimized

    :param df: DataFrame to store
    :param file_name: file name to use
    :param params: will be passed to `df.to_excel()`
        (e.g. `sheets={"my_df": my_df, "other": other_df}, index=False)`
    """
    _store_df(df, file_name, file_type="xlsx", **params)


# Alias for backward compatibility
write_xlsx = store_xlsx


@time_log("loading xlsx file")
def load_xlsx(file_name, **params):
    """
    Load data from a **.xlsx** file into a DataFrame

    :param file_name: file to load
    :param params: will be passed to `pd.read_excel()`
    :return: data from file
    """
    file_path = (_PROJECT_DIR / file_name).resolve()
    info(f"Reading from file {file_path}")
    df = pd.read_excel(file_path, **params)
    return df
