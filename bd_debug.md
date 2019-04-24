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
import beakerx as bx

from pa_lib.file import load_bin, data_files
```

```python
data_files()
```

```python
bd_data_raw = load_bin('bd_data_raw.feather')
bd_data = load_bin('bd_data.feather')
```

```python
bd_kurzz_raw = (bd_data_raw.sort_values('KAMPAGNE_ERFASSUNGSDATUM', ascending=False)
                .loc[:,['ENDKUNDE_NR', 'EK_HB_APG_KURZZ', 'HAUPTBETREUER', 'VERKAUFSBERATER']])
```

```python
bd_kurzz = (bd_data.sort_values('KAMPAGNE_ERFASSUNGSDATUM', ascending=False)
            .loc[:,['ENDKUNDE_NR', 'EK_HB_APG_KURZZ', 'HAUPTBETREUER', 'VERKAUFSBERATER']])
```

```python
ek_vb_raw = bd_kurzz_raw.groupby('ENDKUNDE_NR').agg(lambda x: ', '.join(np.unique(list(map(str, x)))))
```

```python
ek_vb = bd_kurzz.groupby('ENDKUNDE_NR').agg(lambda x: ', '.join(np.unique(list(map(str, x)))))
```

```python
bx.TableDisplay(ek_vb)
```

```python
tmp = bd_data_raw.loc[(bd_data_raw.EK_HB_APG_KURZZ.isna()) & 
                      (bd_data_raw.SEGMENT == 'APG|SGA') &
                      (bd_data_raw.EK_AKTIV == 1) &
                      (bd_data_raw.NETTO > 0)].sort_values('KAMPAGNE_ERFASSUNGSDATUM', ascending=False)

bx.TableDisplay(tmp)
```

```python

```
