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

<!-- #region {"pycharm": {}} -->
# Partnervertrag Analyse: Datenpräparation
<!-- #endregion -->

```python pycharm={"is_executing": false}
%load_ext autoreload
%autoreload

import pandas as pd
import qgrid

from pa_lib.file  import store_bin
from pa_lib.data  import desc_col, as_dtype, as_date, split_date_iso
from pa_lib.util  import obj_size
from pa_lib.types import dtFactor
from pa_lib.sql   import query
from pa_lib.ora   import Connection
from pa_lib.log   import info

# display long columns completely
pd.set_option('display.max_colwidth', 200)
```

<!-- #region {"pycharm": {}} -->
## Daten einlesen
<!-- #endregion -->

```python pycharm={}
pv_query = query('pv_2')
```

```python pycharm={}
info('Starting PV query on APC Prod instance')
with Connection('APC_PROD_VDWH1') as c:
    pv_data_raw = c.long_query(pv_query)
info(f'Finished PV query, returned {obj_size(pv_data_raw)} of data: {pv_data_raw.shape}')
```

```python pycharm={}
pv_data_raw.head()
```

```python pycharm={}
desc_col(pv_data_raw, det=True)
```

<!-- #region {"pycharm": {}} -->
## Leerwerte bereinigen, Datentypen korrigieren
<!-- #endregion -->

```python pycharm={}
pv_data_raw = pv_data_raw.dropna(how='any')
```

```python pycharm={}
(obj_size(pv_data_raw), pv_data_raw.shape)
```

```python pycharm={}
pv_data_raw = pv_data_raw.pipe(as_dtype, dtFactor, incl_dtype='object')
```

```python pycharm={}
(obj_size(pv_data_raw), pv_data_raw.shape)
```

```python pycharm={}
desc_col(pv_data_raw, det=True)
```

```python pycharm={}
pv_data_raw.head()
```

<!-- #region {"pycharm": {}} -->
## Netto = 0 ausfiltern, sortieren
<!-- #endregion -->

```python pycharm={}
pv_data = (pv_data_raw.query('AUS_NETTO_NETTO > 0')
           .sort_values(['JAHR_KW', 'PV_NR'])
           .reset_index(drop=True))
```

```python pycharm={}
desc_col(pv_data, det=True)
```

## Vertragsinformationen extrahieren

```python
pv_idx = pv_data.groupby('PV_NR', as_index=True)
```

```python
pv_info = pv_idx.agg({'PV_TITEL': 'first', 'RES_BRUTTO': 'sum', 'RES_NETTO_NETTO': 'sum', 'AUS_BRUTTO': 'sum', 'AUS_NETTO_NETTO': 'sum', 'PARTNER_NR': 'last', 'PARTNER': 'last',
                      'JAHR_KW': ['min', 'max']})
pv_info.columns = 'Titel totalResBrutto, totalResNettoNetto totalAusBrutto totalAusNettoNetto partnerNr Partner firstKw lastKw'.split()
```

```python
desc_col(pv_info, det=True)
```

```python
qgrid.show_grid(pv_info.loc[:,'Titel NettoNetto_Aus_2017 NettoNetto_Aus_2018 NettoNetto_Aus_2019'.split()])
```

#### Jahres-Nettoumsätze

```python
pvYearANetto = pv_data.groupby(['PV_NR', 'JAHR'], observed=True, as_index=False)[['AUS_NETTO_NETTO']].agg('sum')
pvYearRNetto = pv_data.groupby(['PV_NR', 'JAHR'], observed=True, as_index=False)[['RES_NETTO_NETTO']].agg('sum')
pvANetto = pvYearANetto.pivot(index='PV_NR', columns='JAHR', values='AUS_NETTO_NETTO').fillna(0).add_prefix('NettoNetto_Aus_')
pvRNetto = pvYearRNetto.pivot(index='PV_NR', columns='JAHR', values='RES_NETTO_NETTO').fillna(0).add_prefix('NettoNetto_Res_')
```

```python
pv_info = pv_info.merge(pvANetto, on='PV_NR').merge(pvRNetto, on='PV_NR')
```

<!-- #region {"pycharm": {}} -->
## Daten speichern
<!-- #endregion -->

```python pycharm={}
store_bin(pv_data, 'pv_data.feather')
store_bin(pv_info, 'pv_info.feather')
```
