# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

_file_dir = Path.cwd()
_parent_dir = _file_dir.parent
sys.path.append(str(_parent_dir))

# Imports
import pandas as pd
import random
from concurrent.futures import ProcessPoolExecutor as Pool, as_completed
from joblib import Parallel, delayed
import multiprocessing


pool = Pool(max_workers=8)

# Test code
n_rows = 10_000_000
df = pd.DataFrame(
    dict(
        cls=random.choices("abcdefgh", k=n_rows),
        nr=random.choices([1, 2, 3, 4, 5, 6], k=n_rows),
    )
)


def fun(df):
    return df[["nr"]].sum()


def default_grp_apply(df):
    result = df.groupby("cls").apply(fun)
    return result


def parallel_grp_apply(df):
    jobs = []
    results = []
    for name, group in df.groupby("cls"):
        job = pool.submit(fun, group)
        jobs.append(job)
    for finished in as_completed(jobs):
        r = finished.result()
        results.append(r)
    return pd.concat(results)


def job_grp_apply(df_grouped, func):
    ret_lst = Parallel(n_jobs=multiprocessing.cpu_count())(
        delayed(func)(group) for name, group in df_grouped
    )
    return pd.concat(ret_lst)
