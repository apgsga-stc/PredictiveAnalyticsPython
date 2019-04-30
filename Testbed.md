---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.1'
      jupytext_version: 1.1.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from pandarallel import pandarallel

from pa_lib import file

pandarallel.initialize(nb_workers=8)
```

```python
df = pd.DataFrame(dict(id=[1,1,2,2,3,3,4,4], nr=[1,2,3,4,5,6,7,8]))
```

```python
def f(df):
    return df.agg(['sum', 'min', 'max'])

res1 = df.groupby('id').apply(f)
```

```python
res2 = df.groupby('id').parallel_apply(f)
res1.equals(res2)
```

```python
import pandas as pd
import itertools
import time
import multiprocessing
from typing import Callable, Tuple, Union

def groupby_parallel(groupby_df: pd.core.groupby.DataFrameGroupBy,
                     func: Callable[[Tuple[str, pd.DataFrame]], Union[pd.DataFrame, pd.Series]],
                     num_cpus: int=multiprocessing.cpu_count() - 1,
                     logger: Callable[[str], None]=print) -> pd.DataFrame:
    """Performs a Pandas groupby operation in parallel.
    Example usage:
        import pandas as pd
        df = pd.DataFrame({'A': [0, 1], 'B': [100, 200]})
        df.groupby(df.groupby('A'), lambda row: row['B'].sum())
    Authors: Tamas Nagy and Douglas Myers-Turnbull
    """
    start = time.time()
    logger("\nUsing {} CPUs in parallel...".format(num_cpus))
    with multiprocessing.Pool(num_cpus) as pool:
        queue = multiprocessing.Manager().Queue()
        result = pool.starmap_async(func, [(name, group) for name, group in groupby_df])
        cycler = itertools.cycle('\|/―')
        while not result.ready():
            logger("Percent complete: {:.0%} {}".format(queue.qsize()/len(groupby_df), next(cycler)), end="\r")
            time.sleep(0.4)
        got = result.get()
    logger("\nProcessed {} rows in {:.1f}s".format(len(got), time.time() - start))
    return pd.concat(got)
```

```python
df = pd.DataFrame({'A': [0, 1, 0, 1], 'B': [100, 200, 300, 400]})
def f(df):
    return df.loc[:,'B'].sum()
groupby_parallel(df.groupby('A'), f)
```
