## make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

from pa_lib.file import (
    project_dir,
    load_xlsx,
    store_bin,
)
from pa_lib.data import as_dtype, dtFactor
from pa_lib.util import cap_words

with project_dir("spr_plus"):
    spr_data = (
        load_xlsx("2019_11_13_VSTD_DIGITAL_2019_2020_APG.xlsx", sheet_name="Daten")
        .melt(
            id_vars="WT FLAECHE_ID ANBIETER SPR_FLAECHE_ID".split(),
            var_name="Time",
            value_name="Count",
        )
        .pipe(as_dtype, dtFactor, incl_dtype="object")
        .rename(mapper=lambda name: cap_words(name, sep="_"), axis="columns")
    )

    rail_displays = (
        load_xlsx("flaechen_rail_epanel.xlsx")
        .append(load_xlsx("flaechen_rail_eboard.xlsx"), ignore_index=True, sort=False)
        .drop(["Strasse", "Haus-Nr."], axis="columns")
        .pipe(as_dtype, dtFactor, incl_dtype="object")
    )

spr_data_annotated = spr_data.merge(
    rail_displays, how="left", left_on="Flaeche_ID", right_on="Nummer"
)

with project_dir("axinova"):
    store_bin(spr_data_annotated, "spr_data.feather")
