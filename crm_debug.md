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
from pa_lib.file import load_bin, data_files
```

```python
data_files(pattern = '*crm*')
```

```python
crm_data = load_bin('crm_data_vkprog.feather')
```

```python
crm_data.query('ENDKUNDE_NR == "124677"').sort_values('DATUM', ascending=False)
```

```python
crm_data.dtypes
```

```python

```
