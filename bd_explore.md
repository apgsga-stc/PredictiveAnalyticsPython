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
from pa_lib.df   import calc_col_partitioned, clean_up_categoricals, flatten, replace_col, cond_col, desc_col
from pa_lib.util import obj_size
from pa_lib.log  import time_log

# display long columns completely
pd.set_option('display.max_colwidth', 200)
```

```python
data_files()
```

```python
bd = load_bin('bd_data_vkprog.feather')
```

```python
desc_col(bd, True)
```

#### Set up plotting

```python
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
plt.rcParams['figure.dpi'] = 90
plt.rcParams['figure.figsize'] = [15, 10]
```

```python
data = bd.loc[bd.NETTO > 0].pipe(clean_up_categoricals)
```

### Plots

```python
plt.yscale('log')
sns.violinplot(x=data.KAMP_BEGINN_JAHR, y=data.NETTO)
```

```python
sns.countplot(data.SEGMENT)
```

```python
plt.yscale('log')
sns.boxenplot(data=data, x='SEGMENT', y='NETTO', hue='KAMP_BEGINN_JAHR')
```

```python
sns.scatterplot(x=data.BRUTTO, y=data.NETTO)
```

```python
desc_col(data, det=True)
```

```python
sns.lineplot(data=data.loc[(data.KAMP_ERFASS_JAHR > 2014) & (data.KAMP_ERFASS_JAHR < 2019)], 
             x='KAMP_ERFASS_KW_2', y='NETTO', hue='KAMP_ERFASS_JAHR', legend=False)
```
