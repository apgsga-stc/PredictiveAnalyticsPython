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

# Libraries & Settings

```python
%load_ext autoreload
%autoreload

import pandas as pd
import numpy as np
import qgrid
from datetime import datetime as dtt

from pa_lib.file import data_files, load_bin, store_bin, load_csv, write_xlsx, load_xlsx
from pa_lib.data import (calc_col_partitioned, clean_up_categoricals, flatten, 
                         replace_col, cond_col, desc_col, unfactorize, as_dtype, flatten_multi_index_cols)
from pa_lib.util import obj_size, cap_words
from pa_lib.log  import time_log, info
from pa_lib.vis import dive

# display long columns completely, show more rows
pd.set_option('display.max_colwidth', 200)
pd.set_option('display.max_rows', 200)

def qshow(df, fit_width=False):
    return qgrid.show_grid(df, grid_options={'forceFitColumns': fit_width, 'fullWidthRows': False})
```

# Read data file

```python
data_files('PP*')
```

```python
ppi_data = (load_csv('PPI_Pivot_2019_AGH.csv', sep=';', encoding='cp1252')
            .rename(mapper=lambda name: cap_words(name, sep='_'), axis='columns'))
```

```python
desc_col(ppi_data)
```

# Add columns
* Sum of answer weights per campaign
* Scaled answers (divided by sum of answer weights)
* Raw (uncorrected) answer values, will be 0 or 1
* Remap "Marke_Bekanntheit" [1..5] to "Marke_Bekannt": 'unknown' (NA), 'not' (1), 'low' (2, 3), 'high' (4, 5)

```python
ppi_data.loc[:,'Kamp_Gew_Sum'] = ppi_data.groupby('Ppi_NR')['Gewichtung'].transform(sum)
ppi_data.loc[:,'Befr_Erinn_Prz'] = ppi_data.Befr_Erinnerung / ppi_data.Kamp_Gew_Sum
ppi_data.loc[:,'Befr_Zuord_Prz'] = ppi_data.Befr_Zuordnung / ppi_data.Kamp_Gew_Sum
ppi_data.loc[:,'Befr_Erinn_Raw'] = (ppi_data.Befr_Erinnerung / ppi_data.Gewichtung).astype('int')
ppi_data.loc[:,'Befr_Zuord_Raw'] = (ppi_data.Befr_Zuordnung / ppi_data.Gewichtung).astype('int')

ppi_data.loc[:,'Marke_Bekannt'] = ppi_data.Marke_Bekanntheit.map({1: 'not', 2: 'low', 3: 'low', 4: 'high', 5: 'high'}).fillna('unknown')

ppi_data.head(10)
```

```python
qshow(ppi_data)
```

```python
write_xlsx(ppi_data, 'ppi_data.xlsx', 'data')
```

# Check distribution of answers

```python
ppi_data = load_xlsx('ppi_data.xlsx')
```

### Crosstable (0 = No, 1 = Yes)

```python
pd.crosstab(index=ppi_data.Befr_Erinn_Raw, columns=ppi_data.Befr_Zuord_Raw, margins=True)
```

### What is it with those two cases?

```python
ppi_data.query('Befr_Erinn_Raw == 0 and Befr_Zuord_Raw == 1').T
```

# Aggregation


#### Define aggregation funtion, incl. confidence intervals

```python
import statsmodels.stats.api as sms
import statsmodels.emplike.descriptive as sed

def conf_int(s, alpha=0.05, low_limit=-1e10, high_limit=1e10, method='emplike'):
    if method=='t':
      ci = (sms.DescrStatsW(s).tconfint_mean(alpha=alpha) 
            if len(s) > 1 else (np.NaN, np.NaN))           # assumes t-Distribution
    elif method == 'emplike':
      ci = (sed.DescStatUV(s).ci_mean(sig=alpha) 
            if len(s) > 1 else (np.NaN, np.NaN))           # non-parametric, slower
    else:
      raise ValueError('method parameter must be one of ("t", "emplike")')
    return (max(ci[0], low_limit), min(ci[1], high_limit))

def aggregate_ppi(df, by_col, ci_method='emplike'):
    def first(seq):
        return seq[0]

    def second(seq):
        return seq[1]

    def ci(seq):
        return conf_int(seq, alpha=0.05, method=ci_method, low_limit=0, high_limit=1)

    summary = (df.groupby(by_col, observed=True)
               .agg({'Ppi_NR': 'nunique',
                     'Befr_Erinn_Prz': ['mean', ci], 
                     'Befr_Zuord_Prz': ['mean', ci], 
                     'Nettowirk_Prz': ['mean', ci]})
               .set_axis('Umfragen Erinn_mean Erinn_ci Zuord_mean Zuord_ci Nettowirk_mean Nettowirk_ci'.split(), 
                         axis='columns', inplace=False))
    result = summary.loc[:,['Umfragen']]
    for col in 'Erinn Zuord Nettowirk'.split():
        result[f'{col}_mean'] = summary[f'{col}_mean']
        ci_col = f'{col}_ci'
        result = result.assign(**{f'{col}_low': summary[ci_col].transform(first),
                                  f'{col}_high': summary[ci_col].transform(second)})
    return result
```

## Aggregate on campaign

```python
ppi_kamp_data = (ppi_data.groupby('Ppi_NR')
                 .agg({'Kunde': 'last', 'Kpg_Name': 'last', 'Branche': 'last', 'Gruppe': 'last', 'Kategorie': 'last', 'Std_Publ': 'last', 'Spr_Werbedruck': 'mean',
                       'Befr_Erinn_Prz': 'sum', 'Befr_Zuord_Prz': 'sum'})
                 .eval('Nettowirk_Prz = Befr_Zuord_Prz / Befr_Erinn_Prz')
                 .join(ppi_data.groupby('Ppi_NR').size().rename('Befr_N')))

write_xlsx(ppi_kamp_data, 'ppi_kamp.xlsx', 'data')
ppi_kamp_data.head(10)
```

```python
_ = aggregate_ppi(ppi_kamp_data.reset_index(), by_col='Std_Publ', ci_method='emplike')
write_xlsx(_, 'ppi_kamp_by_std_publ.xlsx', 'data')
_
```

#### Plot answer distributions

```python
import seaborn as sns
import matplotlib.pyplot as plt
%matplotlib inline
```

##### Global distributions

```python
fig = plt.figure(1, figsize=(14,4), tight_layout=False)
plt.subplot(1,3,1)
sns.distplot(ppi_kamp_data.Befr_Erinn_Prz)
plt.subplot(1,3,2)
sns.distplot(ppi_kamp_data.Befr_Zuord_Prz)
plt.subplot(1,3,3)
sns.distplot(ppi_kamp_data.Nettowirk_Prz);
```

##### Distribution per Std_Publ

```python
g = sns.FacetGrid(data=ppi_kamp_data, col="Std_Publ", col_wrap=4, height=4)
g.map(sns.distplot, 'Befr_Erinn_Prz');
```

```python
g = sns.FacetGrid(data=ppi_kamp_data, col="Std_Publ", col_wrap=4, height=4)
g.map(sns.distplot, 'Nettowirk_Prz');
```

#### Check empties on "Marken_Bekanntheit"

```python
def empty(s):
    return all(s.isna())

def full(s):
    return all(s.notna())

def mixed(s):
    return any(s.isna()) and any(s.notna())

display(ppi_data.groupby('Jahr')[['Marke_Bekanntheit']].agg([empty, mixed, full]))
ppi_data.groupby('Jahr')[['Marke_Bekanntheit']].agg([empty, mixed, full]).sum(axis=0)
```

## Aggregate on campaign / high-low Marken-Bekanntheit

```python
ppi_kamp_hilo_data = (ppi_data.groupby(['Ppi_NR', 'Marke_Bekannt'], observed=True)
                      .agg({'Kunde': 'last', 'Kpg_Name': 'last', 'Branche': 'last', 'Gruppe': 'last', 'Kategorie': 'last', 'Std_Publ': 'last',  'Spr_Werbedruck': 'mean',
                            'Befr_Erinn_Prz': 'sum', 'Befr_Zuord_Prz': 'sum'})
                      .eval('Nettowirk_Prz = Befr_Zuord_Prz / Befr_Erinn_Prz')
                      .join(ppi_data.groupby(['Ppi_NR', 'Marke_Bekannt'], observed=True).size().rename('Befr_N')))

write_xlsx(ppi_kamp_hilo_data, 'ppi_kamp_hilo.xlsx', 'data')
```

```python
qshow(ppi_kamp_hilo_data)
```

```python
_ = aggregate_ppi(ppi_kamp_hilo_data.reset_index(), by_col=['Std_Publ', 'Marke_Bekannt'], ci_method='t')
write_xlsx(_, 'ppi_kamp_hilo_by_std_publ.xlsx', 'data')
_
```

#### Plot answer distributions

```python
g = sns.catplot(data=ppi_kamp_hilo_data.reset_index(), x='Marke_Bekannt', order=['unknown', 'not', 'low', 'high'], 
                y='Befr_Erinn_Prz', col='Std_Publ', col_wrap=4, height=5, kind='box')
```

```python
g = sns.catplot(data=ppi_kamp_hilo_data.reset_index(), x='Marke_Bekannt', order=['unknown', 'not', 'low', 'high'], 
                y='Befr_Erinn_Prz', col='Std_Publ', col_wrap=4, height=5, kind='swarm')
```

```python
g = sns.catplot(data=ppi_kamp_hilo_data.reset_index(), x='Marke_Bekannt', order=['unknown', 'not', 'low', 'high'], 
                y='Nettowirk_Prz', col='Std_Publ', col_wrap=4, height=5, kind='box')
```

```python
g = sns.catplot(data=ppi_kamp_hilo_data.reset_index(), x='Marke_Bekannt', order=['unknown', 'not', 'low', 'high'], 
                y='Nettowirk_Prz', col='Std_Publ', col_wrap=4, height=5, kind='swarm');
```

# Auswertung Werbedruck vs. Impact

```python
%matplotlib inline 

from pandas.plotting import boxplot

import matplotlib.pyplot as plt
```

```python
ppi_data.boxplot(column='Spr_Werbedruck', by='Jahr', figsize=(10,6))
```

```python
ppi_kamp_hilo_data.columns
```

### Does more Werbedruck result in better Erinnerung?

```python
ppi_kamp_data.plot.scatter(x='Spr_Werbedruck', y='Nettowirk_Prz', figsize=(10,6))
```

### Split by Marke_Bekannt

```python
g = sns.relplot(data=ppi_kamp_hilo_data, 
                x='Spr_Werbedruck', y='Nettowirk_Prz', col='Marke_Bekannt', col_wrap=2, height=5, aspect=2, kind='scatter');
```

### Split by Std_Publ

```python
g = sns.relplot(data=ppi_kamp_data, 
                x='Spr_Werbedruck', y='Nettowirk_Prz', col='Std_Publ', col_wrap=4, height=5, kind='scatter');
```
