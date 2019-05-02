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
%load_ext autoreload
%autoreload

import pandas as pd
import numpy as np
import qgrid
import beakerx as bx
from datetime import datetime as dtt

from pa_lib.file import data_files, load_bin, store_bin, store_excel
from pa_lib.data import calc_col_partitioned, clean_up_categoricals, flatten, replace_col, cond_col, desc_col
from pa_lib.util import obj_size
from pa_lib.log  import time_log
from pa_lib.types import dtFactor

# display long columns completely
pd.set_option('display.max_colwidth', 200)
```

```python
data_files()
```

```python
bd = load_bin('bd_data.feather')
```

```python
desc_col(bd, det=True)
```

#### Filtered data: Only above-zero

```python
bd = bd.loc[(bd.NETTO >= 0)].pipe(clean_up_categoricals)
```

#### Reduced data: Only from 2014, only non-zero

```python
data = bd.loc[(bd.KAMP_ERFASS_JAHR > 2014)].pipe(clean_up_categoricals)
```

#### Set up plotting

```python
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
plt.rcParams['figure.dpi'] = 90
plt.rcParams['figure.figsize'] = [15, 10]
```

### Plots

```python
sns.barplot(data=data, x="SEGMENT", y="NETTO", hue='KAMP_BEGINN_JAHR', estimator=np.sum)
```

```python
sns.barplot(data=data, x="SEGMENT", y="NETTO", hue='KAMP_BEGINN_JAHR', estimator=np.median)
```

```python
plt.yscale('log')
sns.boxenplot(data=data, x='SEGMENT', y='NETTO', hue='KAMP_BEGINN_JAHR')
```

```python
sns.scatterplot(x=bd.BRUTTO, y=bd.NETTO)
```

```python
desc_col(data, det=True)
```

```python
data.KAMP_BEGINN_JAHR.value_counts(dropna=False)
```

```python
data.loc[data.KAMP_BEGINN_JAHR.isna()]['KAMP_ERFASS_JAHR'].value_counts()
```

```python
sns.lineplot(x=data.KAMP_ERFASS_KW_2, y=data.NETTO, hue=data.KAMP_ERFASS_JAHR, legend=False)
```

### Netto Reservation und Aushang per Endkunde und KW2

```python
from concurrent.futures import ProcessPoolExecutor

def sum_calc(param):
    (df, col_year, col_week) = param
    return (df.groupby(['ENDKUNDE_NR', col_year, col_week], observed=False, as_index=False)[['NETTO']].agg('sum'))

with time_log('calculating sums'):
    with ProcessPoolExecutor(max_workers=2) as executor:
        (data_res, data_aus) = tuple(executor.map(sum_calc, [(data, 'KAMP_ERFASS_JAHR', 'KAMP_ERFASS_KW_2'), 
                                                             (data, 'KAMP_BEGINN_JAHR', 'KAMP_BEGINN_KW_2')]))

    data_by_week = (data_res.merge(data_aus,  
                                   left_on=['ENDKUNDE_NR', 'KAMP_ERFASS_JAHR', 'KAMP_ERFASS_KW_2'], 
                                   right_on=['ENDKUNDE_NR', 'KAMP_BEGINN_JAHR', 'KAMP_BEGINN_KW_2'], 
                                   how='outer', suffixes=('_res', '_aus'))
                            .rename({'KAMP_ERFASS_JAHR': 'Jahr', 'KAMP_ERFASS_KW_2': 'Kw', 'NETTO_res': 'Res', 'NETTO_aus': 'Aus'}, 
                                    axis='columns'))

    data_by_week = (data_by_week.fillna({'Jahr': data_by_week.KAMP_BEGINN_JAHR, 'Kw': data_by_week.KAMP_BEGINN_KW_2, 'Res': 0, 'Aus': 0})
                                .drop(['KAMP_BEGINN_JAHR', 'KAMP_BEGINN_KW_2'], axis='columns')
                                .sort_values(['Jahr', 'ENDKUNDE_NR', 'Kw'])
                                .reset_index(drop=True))

store_bin(data_by_week, 'bd_by_week.feather')
```

```python
data_by_week = load_bin('bd_by_week.feather')
```

```python
desc_col(data_by_week, det=True)
```

```python
data_non_zero = (data_by_week
                 .query('Res > 0 or Aus > 0')
                 .pipe(clean_up_categoricals))

ek_minmax = (data_non_zero
             .assign(Jahr_Kw = data_non_zero.Jahr.astype('str').str.cat(
                               data_non_zero.Kw.astype('str'), sep='_')
                               .str.replace(r'_(\d)$', r'_0\g<1>') # make Kw two digits for sorting
                               .astype('str'))
             .drop(['Kw', 'Jahr'], axis='columns')
             .groupby('ENDKUNDE_NR')
             .agg({'Res': 'sum', 'Aus': 'sum', 'Jahr_Kw': ['min', 'max']}))
```

```python
cols = [f'{fld}_{agg}' for (fld, agg) in ek_minmax.columns.to_flat_index()]
ek_minmax.set_axis(labels=cols, axis='columns', inplace=True)
```

```python
qgrid.set_grid_option('minVisibleRows', 1)
qgrid.set_grid_option('maxVisibleRows', 20)

qgrid.show_grid(ek_minmax)
```

### Vollständige Daten per Woche

```python
from concurrent.futures import ProcessPoolExecutor

def sum_calc(param):
    (df, col_year, col_week) = param
    return (df.groupby(['ENDKUNDE_NR', col_year, col_week], observed=True, as_index=False)[['NETTO']].agg('sum'))

with time_log('calculating sums'):
    with ProcessPoolExecutor(max_workers=2) as executor:
        (bd_res,bd_aus) = tuple(executor.map(sum_calc, [(bd, 'KAMP_ERFASS_JAHR', 'KAMP_ERFASS_KW_2'), 
                                                        (bd, 'KAMP_BEGINN_JAHR', 'KAMP_BEGINN_KW_2')]))

    bd_by_week = (bd_res.merge(bd_aus,  
                               left_on=['ENDKUNDE_NR', 'KAMP_ERFASS_JAHR', 'KAMP_ERFASS_KW_2'], 
                               right_on=['ENDKUNDE_NR', 'KAMP_BEGINN_JAHR', 'KAMP_BEGINN_KW_2'], 
                               how='outer', suffixes=('_res', '_aus'))
                        .rename({'KAMP_ERFASS_JAHR': 'Jahr', 'KAMP_ERFASS_KW_2': 'Kw', 'NETTO_res': 'Res', 'NETTO_aus': 'Aus'}, 
                                axis='columns'))

    bd_by_week = (bd_by_week.fillna({'Jahr': bd_by_week.KAMP_BEGINN_JAHR, 'Kw': bd_by_week.KAMP_BEGINN_KW_2, 'Res': 0, 'Aus': 0})
                            .drop(['KAMP_BEGINN_JAHR', 'KAMP_BEGINN_KW_2'], axis='columns')
                            .sort_values(['Jahr', 'ENDKUNDE_NR', 'Kw'])
                            .reset_index(drop=True))

store_bin(bd_by_week, 'bd_long_by_week.feather')
```

```python
bd_by_week = load_bin('bd_long_by_week.feather')
```

```python
desc_col(bd_by_week, det=True)
```

```python
qgrid.set_grid_option('minVisibleRows', 1)
qgrid.set_grid_option('maxVisibleRows', 20)

qgrid.show_grid(bd_by_week)
```

```python
qgrid.show_grid(bd.loc[(bd.ENDKUNDE_NR==483063) & (bd.KAMP_BEGINN_JAHR==2014) & (bd.KAMP_BEGINN_KW_2.isin([23, 45]))].transpose())
```

```python
bd_non_zero = (bd_by_week
                 .query('Res > 0 or Aus > 0')
                 .pipe(clean_up_categoricals))

ek_minmax = (bd_non_zero
             .assign(Jahr_Kw = bd_non_zero.Jahr.astype('str').str.cat(
                               bd_non_zero.Kw.astype('str'), sep='_')
                               .str.replace(r'_(\d)$', r'_0\g<1>') # make Kw two digits for sorting
                               .astype('str'))
             .drop(['Kw', 'Jahr'], axis='columns')
             .groupby('ENDKUNDE_NR')
             .agg({'Res': 'sum', 'Aus': 'sum', 'Jahr_Kw': ['min', 'max']}))
```

```python
cols = [f'{fld}_{agg}' for (fld, agg) in ek_minmax.columns.to_flat_index()]
ek_minmax.set_axis(labels=cols, axis='columns', inplace=True)
```

```python
ek_minmax.reset_index(inplace=True)
store_bin(ek_minmax, 'bd_ek_minmax.feather')
```

```python
qgrid.set_grid_option('minVisibleRows', 1)
qgrid.set_grid_option('maxVisibleRows', 20)

qgrid.show_grid(ek_minmax.query('J'))
```
