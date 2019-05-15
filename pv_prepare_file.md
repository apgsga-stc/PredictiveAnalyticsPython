---
jupyter:
  jupytext:
    formats: ipynb,md
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
# Budgetbuchung Analyse: Datenpräparation
<!-- #endregion -->

```python pycharm={"is_executing": false}
%load_ext autoreload
%autoreload

import pandas as pd

from pa_lib.file  import data_files, load_csv, store_bin
from pa_lib.data  import desc_col, as_dtype, as_date, split_date_iso
from pa_lib.util  import obj_size
from pa_lib.types import dtFactor

# display long columns completely
pd.set_option('display.max_colwidth', 200)
```

<!-- #region {"pycharm": {}} -->
## Daten einlesen
<!-- #endregion -->

```python pycharm={}
data_files()
```

```python pycharm={}
pv_bd = load_csv('pv_bd.zip', delimiter=';', encoding='cp1252', dtype='object')
```

```python pycharm={}
pv_bd.head()
```

```python pycharm={}
(obj_size(pv_bd), pv_bd.shape)
```

```python pycharm={}
desc_col(pv_bd)
```

<!-- #region {"pycharm": {}} -->
## Spalten umbenennen, Leerwerte bereinigen, Datentypen korrigieren
<!-- #endregion -->

```python pycharm={}
pv_bd.columns = ['ResDatum', 'AushangBeginn', 'PvPosNr', 'PartnerNr', 'PartnerName',
       'PvNr', 'PvTitel', 'optBrutto', 'optNetto', 'optNettoNetto']
pv_bd = pv_bd.dropna(how='any')
```

```python pycharm={}
(obj_size(pv_bd), pv_bd.shape)
```

```python pycharm={}
pv_bd.loc[:,:] = (pv_bd
                  .pipe(as_dtype, 'int', incl_pattern='.*Nr.*')
                  .pipe(as_dtype, 'float', incl_pattern='.*tto')
                  .pipe(as_date, format_str='%d.%m.%Y', incl_col=('ResDatum', 'AushangBeginn'))
                  .pipe(as_dtype, dtFactor, incl_dtype='object'))
```

```python pycharm={}
(obj_size(pv_bd), pv_bd.shape)
```

```python pycharm={}
desc_col(pv_bd, det=True)
```

```python pycharm={}
pv_bd.head()
```

<!-- #region {"pycharm": {}} -->
## Netto = 0 ausfiltern, sortieren, Geschäftsjahr und -woche für Aushang und Reservation berechnen
<!-- #endregion -->

```python pycharm={}
pv_bd = (pv_bd.query('optNettoNetto > 0')
         .sort_values('AushangBeginn')
         .pipe(split_date_iso, dt_col='ResDatum', yr_col='RJahr', kw_col='RKw')
         .pipe(split_date_iso, dt_col='AushangBeginn', yr_col='AJahr', kw_col='AKw')
         .reset_index(drop=True))
```

```python pycharm={}
desc_col(pv_bd, det=True)
```

## Vertragsinformationen extrahieren

```python
pv_idx = pv_bd.sort_values(['PvNr', 'ResDatum']).groupby('PvNr', as_index=True)
```

```python
pv_info = pv_idx.agg({'PvTitel': 'first', 'optNettoNetto': 'sum', 'PartnerNr': 'nunique', 'PartnerName': 'last', 'PvPosNr': 'nunique',
                      'ResDatum': ['min', 'max'], 'AushangBeginn': ['min', 'max']})
pv_info.columns = 'Titel totalNetto nPartner Partner nPos firstRes lastRes firstAus lastAus'.split()
```

```python
desc_col(pv_info, det=True)
```

#### Mehrfach-Partner: Namen zusammenfügen (Reihenfolge wie in Daten)

```python
pv_info.assign(allPartner = pv_info.Partner, inplace=True)
multi_partner = pv_info.nPartner > 1
pv_multi_prtn = pv_info.loc[multi_partner].index.values
pv_info.loc[multi_partner, 'allPartner'] = (pv_bd[pv_bd.PvNr.isin(pv_multi_prtn)].groupby('PvNr')['PartnerName']
                                                .apply(lambda x: ' | '.join(x.unique())))
```

#### Jahres-Nettoumsätze

```python
pvYearANetto = pv_bd.groupby(['PvNr', 'AJahr'], observed=True, as_index=False)[['optNettoNetto']].agg('sum')
pvYearRNetto = pv_bd.groupby(['PvNr', 'RJahr'], observed=True, as_index=False)[['optNettoNetto']].agg('sum')
pvANetto = pvYearANetto.pivot(index='PvNr', columns='AJahr', values='optNettoNetto').fillna(0).add_prefix('Netto_Aus_')
pvRNetto = pvYearRNetto.pivot(index='PvNr', columns='RJahr', values='optNettoNetto').fillna(0).add_prefix('Netto_Res_')
```

```python
pv_info = pv_info.merge(pvANetto, on='PvNr').merge(pvRNetto, on='PvNr')
```

<!-- #region {"pycharm": {}} -->
## Konstellation Verträge/Positionen prüfen
<!-- #endregion -->

<!-- #region {"pycharm": {}} -->
#### Gibt es Vertragspositionen mit mehr als einem unterschiedlichen Vertrag? (Wäre nicht toll)
<!-- #endregion -->

```python pycharm={}
pv_bd.groupby(['PvPosNr'], observed=True)[['PvNr']].agg('nunique').query('PvNr > 1')
```

<!-- #region {"pycharm": {}} -->
## Daten speichern
<!-- #endregion -->

```python pycharm={}
store_bin(pv_bd, 'pv_data_file_raw.feather')
pv_data = pv_bd.drop(['PvPosNr', 'PartnerNr', 'PartnerName', 'PvTitel', 'optBrutto', 'optNetto'], axis='columns')
store_bin(pv_data, 'pv_data_file.feather')
store_bin(pv_info, 'pv_info_file.feather')
```
