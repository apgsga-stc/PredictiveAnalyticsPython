# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import pandas as pd
import itertools
import random

from pa_lib.log import time_log
from pa_lib.file import store_pickle, load_pickle, project_dir


########################################################################################
# GLOBALS
########################################################################################
CAP_PROJECT_DIR: str = "capacity"
N_FLAECHEN: int = 1000


########################################################################################
# FUNCTIONS
########################################################################################
def simulate_cap_data(n_flaechen: int = N_FLAECHEN) -> pd.DataFrame:
    global N_FLAECHEN
    
    if n_flaechen != N_FLAECHEN:
        N_FLAECHEN = n_flaechen
    random.seed(42)
    years = range(2016, 2020)
    year_weights = {y: random.random() for y in years}
    kw = range(1, 53)
    kw_weights = {w: random.random() for w in kw}
    fl_nr = range(10000, 10000 + n_flaechen)
    fl_weights = {f: random.random() for f in fl_nr}
    data = pd.DataFrame.from_records(
        itertools.product(years, kw, fl_nr), columns=["year", "kw", "fl_nr"]
    )
    data["prob"] = data.apply(
        lambda rec: (
            year_weights[rec.year] + kw_weights[rec.kw] + fl_weights[rec.fl_nr]
        )
        / 3,
        axis=1,
    )
    data["belegt"] = (data["prob"] > 0.5).astype("int")
    return data


def store_cap_data(data: pd.DataFrame, file_name: str) -> None:
    with project_dir(CAP_PROJECT_DIR):
        store_pickle(data, file_name=file_name)


def load_cap_data(file_name: str) -> pd.DataFrame:
    with project_dir(CAP_PROJECT_DIR):
        data = load_pickle(file_name)
    return data


########################################################################################
# TEST CODE
########################################################################################
if __name__ == "__main__":
    with time_log("simulating data"):
        cap_data = simulate_cap_data(n_flaechen=1000)

    store_cap_data(cap_data, "simulation.pkl")
