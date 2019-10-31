# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

from pa_lib.file import store_bin, project_dir, load_csv
from pa_lib.data import as_dtype, dtFactor


def mf_int(field: str):
    """
    Convert a MediaFocus-formatted number to an integer
    """
    return int(field.replace("’", ""))


# Prepare parameter sets for the different export files
br_kw_col_names = ["Jahr", "Branche"] + [f"KW_{kw:02}" for kw in range(1, 54)]
br_kw_col_converters = [str, str] + [mf_int] * 53
br_kw_params = dict(
    encoding="cp1252",
    skiprows=11,
    usecols=range(55),
    sep=";",
    names=br_kw_col_names,
    converters=dict(zip(br_kw_col_names, br_kw_col_converters)),
)

br_pg_kw_col_names = ["Jahr", "Branche", "Produktgruppe"] + [
    f"KW_{kw:02}" for kw in range(1, 54)
]
br_pg_kw_col_converters = [str, str, str] + [mf_int] * 53
br_pg_kw_params = br_kw_params.copy()
br_pg_kw_params.update(
    dict(
        skiprows=10,
        usecols=range(56),
        names=br_pg_kw_col_names,
        converters=dict(zip(br_pg_kw_col_names, br_pg_kw_col_converters)),
    )
)

br_wbt_kw_col_names = ["Branche", "Werbungtreibender"] + [
    f"KW_{kw:02}" for kw in range(1, 54)
]
br_wbt_kw_col_converters = br_kw_col_converters.copy()
br_wbt_kw_params = br_kw_params.copy()
br_wbt_kw_params.update(
    dict(
        skiprows=10,
        names=br_wbt_kw_col_names,
        converters=dict(zip(br_wbt_kw_col_names, br_wbt_kw_col_converters)),
    )
)

# Read export files using parameter sets, store as binary files
with project_dir("MediaFocus"):
    br_kw = (
        load_csv("Branchen_KW_10y.csv", **br_kw_params)
        .pipe(as_dtype, dtFactor, incl_dtype="object")
        .melt(id_vars=["Jahr", "Branche"], var_name="KW", value_name="Brutto")
    )
    br_pg_kw = (
        load_csv("BranchenProdgrp_KW_10y.csv", **br_pg_kw_params)
        .pipe(as_dtype, dtFactor, incl_dtype="object")
        .melt(
            id_vars=["Jahr", "Branche", "Produktgruppe"],
            var_name="KW",
            value_name="Brutto",
        )
    )

    br_wbt_kw = (
        load_csv("BranchenWbt_KW_4y.csv", **br_wbt_kw_params)
        .pipe(as_dtype, dtFactor, incl_dtype="object")
        .melt(
            id_vars=["Branche", "Werbungtreibender"], var_name="KW", value_name="Brutto"
        )
    )
    store_bin(br_kw, "branchen_kw_10y.feather")
    store_bin(br_pg_kw, "branchen_pg_kw_10y.feather")
    store_bin(br_wbt_kw, "branchen_wbt_kw.feather")