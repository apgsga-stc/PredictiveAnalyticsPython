#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:33:01 2019
File handling for PA data
@author: kpf
"""
import pandas as pd
import numpy as np
import sys
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime as dtt
from functools import partial

from pa_lib.const import PA_DATA_DIR
from pa_lib.log import time_log, info
from pa_lib.util import format_size
from pa_lib.data import flatten_multi_index_cols

flatten_multi_cols = partial(flatten_multi_index_cols, sep="|")

# global state: sub-directory of PA_DATA_DIR we're working in
_project_dir = PA_DATA_DIR  # initialize to base data directory


def set_project_dir(dir_name):
    """Set current project dir under base data directory to work in.
       Will be created if not present"""
    global _project_dir
    new_project_dir = PA_DATA_DIR / dir_name
    new_project_dir.mkdir(exist_ok=True)
    if new_project_dir.is_dir():
        _project_dir = new_project_dir
    else:
        raise FileExistsError(
            f"Can't create directory '{dir_name}': File exists in {PA_DATA_DIR}"
        )


def reset_project_dir():
    """Reset current project dir to initial status (base data directory)"""
    global _project_dir
    _project_dir = PA_DATA_DIR


def get_project_dir():
    """Currently set project dir"""
    return _project_dir


@contextmanager
def project_dir(dir_name):
    """Temporarily switch into another project dir"""
    previous_project_dir = get_project_dir()
    set_project_dir(dir_name)
    try:
        yield
    finally:
        set_project_dir(previous_project_dir)


###############################################################################
def file_list(path=".", pattern="*.*", sort="name", desc=False, do_format=True):
    """DataFrame(name, size, mtime) for all files in path, filtered by pattern,
       sorted by 'sort' and 'desc'"""
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


def data_files(pattern="[!.]*.*", sort="name", **kwargs):
    return file_list(_project_dir, pattern, sort, **kwargs).set_index(sort)


###############################################################################
def file_size(file_path, do_format=True):
    nbytes = Path(file_path).stat().st_size
    if do_format:
        return format_size(nbytes)
    else:
        return nbytes


###############################################################################
@time_log("storing text file")
def store_txt(txt, file_name, **params):
    file_path = (_project_dir / file_name).resolve()
    info(f"Writing to file {file_path}")
    with file_path.open("w", **params) as file:
        file.write(txt)
    info(f"Written {file_size(file_path)}")


@time_log("loading text file")
def load_txt(file_name, **params):
    file_path = (_project_dir / file_name).resolve()
    info(f"Reading from file {file_path}")
    with file_path.open("r", **params) as file:
        txt = file.read()
    return txt


###############################################################################
def rm_file(file_name):
    file_path = Path(file_name).resolve()
    info(f"Removing file {file_path}")
    file_path.unlink()


def rm_data_file(file_name):
    file_path = _project_dir / file_name
    rm_file(file_path)


###############################################################################
def _check_index(df):
    # if we have a default index, trash it. If it's more sophisticated, store it
    if df.index.equals(pd.RangeIndex.from_range(range(len(df)))):
        df_checked = df.reset_index(drop=True)
    else:
        df_checked = df.reset_index()
    return df_checked


def _store_df(df, file_name, file_type, **params):
    file_path = (_project_dir / file_name).resolve()
    # Pickle files support all python objects. For other formats, normalize df.
    if file_type != "pickle":
        df_out = df.pipe(_check_index).pipe(flatten_multi_cols)
    else:
        df_out = df.copy()

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
                index = params.pop("index", False)
                df_out.to_csv(file_path, compression=compression, index=index, **params)
            elif file_type == "pickle":
                df_out.to_pickle(file_path, **params)
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
                sheet_name = params.pop("sheet_name", "df")
                index = params.pop("index", False)
                # column widths as max strlength of column's contents,
                # or strlength of column's header if greater
                col_width = np.maximum(
                    df_out.astype("str")
                    .apply(lambda column: max(column.str.len()))
                    .to_list(),
                    list(map(len, df_out.columns)),
                )
                file_path = _project_dir / file_name
                writer = pd.ExcelWriter(file_path, engine="xlsxwriter")
                df_out.to_excel(writer, index=index, sheet_name=sheet_name)
                # Formatting
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]
                ncols = df_out.shape[-1]
                title_cells = (0, 0, 0, ncols - 1)
                bold = workbook.add_format({"bold": True, "align": "left"})
                worksheet.set_row(0, cell_format=bold)
                worksheet.autofilter(*title_cells)
                worksheet.freeze_panes(1, 0)
                # Column autowidth
                for col in range(ncols):
                    worksheet.set_column(col, col, col_width[col] + 1)
                writer.save()
            else:
                raise ValueError(f"Unknown file type '{file_type}'")
        except:
            file_path.unlink()
            if bkup_path.exists():
                bkup_path.replace(file_path)
            raise IOError(f"Failed writing file {file_path}: {sys.exc_info()[1]}")
        else:
            if bkup_path.exists():
                bkup_path.unlink()
            info(f"Written {file_size(file_path)}")


def store_bin(df, file_name, **params):
    """Store df as a 'feather' file in current project directory. **params go to df.to_feather"""
    _store_df(df, file_name, file_type="feather", **params)


@time_log("loading binary file")
def load_bin(file_name, **params):
    file_path = (_project_dir / file_name).resolve()
    info(f"Reading from file {file_path}")
    df = pd.read_feather(file_path, **params)
    return df


def store_pickle(df, file_name, **params):
    """Store df as a 'pickle' file in current project directory. **params go to df.to_pickle"""
    _store_df(df, file_name, file_type="pickle", **params)


@time_log("loading pickle file")
def load_pickle(file_name, **params):
    file_path = (_project_dir / file_name).resolve()
    info(f"Reading from file {file_path}")
    df = pd.read_pickle(file_path, **params)
    return df


def store_csv(df, file_name, **params):
    """Store df as a 'csv' file in current project directory. **params go to df.to_csv
    (e.g. compression="zip", index=False)"""
    _store_df(df, file_name, file_type="csv", **params)


@time_log("loading CSV file")
def load_csv(file_name, **params):
    file_path = (_project_dir / file_name).resolve()
    info(f"Reading from file {file_path}")
    df = pd.read_csv(file_path, low_memory=False, **params)
    return df


def store_hdf(df, file_name, **params):
    """Store df as a 'hdf' file in current project directory. **params go to df.to_hdf
    (e.g. key="df", index=False)"""
    _store_df(df, file_name, file_type="hdf", **params)


@time_log("loading HDF file")
def load_hdf(file_name, **params):
    file_path = (_project_dir / file_name).resolve()
    info(f"Reading from file {file_path}")
    with pd.HDFStore(file_path, mode="r", **params) as ds:
        df = ds["df"]
    return df


def store_xlsx(df, file_name, **params):
    """Store df as a 'xlsx' file in current project directory. **params go to df.to_to_excel
    (e.g. sheet_name="df", index=False)"""
    _store_df(df, file_name, file_type="xlsx", **params)


# Alias for backward compatibility
write_xlsx = store_xlsx


@time_log("loading xlsx file")
def load_xlsx(file_name, **params):
    file_path = (_project_dir / file_name).resolve()
    info(f"Reading from file {file_path}")
    df = pd.read_excel(file_path, **params)
    return df
