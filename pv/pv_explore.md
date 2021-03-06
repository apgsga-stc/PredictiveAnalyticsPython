---
jupyter:
  jupytext:
    cell_metadata_json: true
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.3.2
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region {"toc-hr-collapsed": false} -->
# Budgetbuchung Analyse: Exploration
<!-- #endregion -->

```python
# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))
```

```python
%load_ext autoreload
%autoreload

import pandas as pd
import numpy as np
import qgrid
from datetime import datetime as dtt

from pa_lib.file import data_files, load_bin, store_bin, store_excel, set_project_dir
from pa_lib.data import calc_col_partitioned, clean_up_categoricals, flatten, replace_col, cond_col, desc_col, as_dtype
from pa_lib.util import obj_size
from pa_lib.log  import time_log
from pa_lib.type import dtFactor

# display long columns completely
pd.set_option('display.max_colwidth', 200)
```

```python
set_project_dir('pv')

data_files()
```

```python
pv_data = load_bin('pv_data.feather').sort_values(['PV_NR', 'JAHR_KW']).reset_index(drop=True)
pv_info = load_bin('pv_info.feather').set_index('PV_NR')
```

<!-- #region {"toc-hr-collapsed": false} -->
# Rangliste der Verträge nach Umsatz

Nur Verträge, die seit 2017 jedes Jahr Umsatz generieren, sortiert nach Gesamtumsatz absteigend
<!-- #endregion -->

```python
desc_col(pv_info, det=True)
```

```python
pv_liste = tuple(pv_info.loc[(pv_info.firstKw < 201501) & (pv_info.lastKw >= 201901),:]
#                        .query('Netto_Aus_2014 * Netto_Aus_2015 * Netto_Aus_2016 * Netto_Aus_2017 * Netto_Aus_2018 * Netto_Aus_2019 > 0')
#                        .query('Netto_Res_2014 * Netto_Res_2015 * Netto_Res_2016 * Netto_Res_2017 * Netto_Res_2018 * Netto_Res_2019 > 0')
                        .query('NettoNetto_Aus_2017 * NettoNetto_Aus_2018 * NettoNetto_Aus_2019 > 0')
                        .query('NettoNetto_Res_2017 * NettoNetto_Res_2018 * NettoNetto_Res_2019 > 0')
                        .eval('Sort = NettoNetto_Aus_2017 + NettoNetto_Aus_2018 + NettoNetto_Aus_2019')
                        .sort_values('Sort', ascending=False).index.values)
qgrid.show_grid(pv_info.loc[pv_liste[:20], 'Titel Partner NettoNetto_Aus_2017 NettoNetto_Aus_2018 NettoNetto_Aus_2019'.split()])
```

### Check: Buchungen zu Top-20 Verträgen ansehen

```python
pv_top20 = pv_data.loc[pv_data['PV_NR'].isin(pv_liste[:20])]
qgrid.show_grid(pv_top20.loc[(pv_top20.PV_NR==20199) & (pv_top20.JAHR=='2017')].sort_values(['JAHR', 'KW']))
```

```python
pv_top20.pivot_table(values='AUS_NETTO_NETTO', index='PV_NR', columns='JAHR', aggfunc='sum', 
                     fill_value=0, margins=True).astype('int').sort_values('All', ascending=False)
```

# Aggregationen hinzufügen


#### Jahres-Summenkurven für Nettoumsatz pro Partner/Vertrag/Position

Fürs aktuelle Jahr werden die Jahressummen vom letzten Jahr benutzt. Dies impliziert, dass derselbe Umsatz erwartet wird. 
So zeigt die Summenkurve die Zielerreichung relativ zum Vorjahr.

```python
def make_year_grp_sumcurve(df, year_col, grp_col, data_col, prefix=''):
    # rowmasks for this/last year
    this_year = (df[year_col] == dtt.today().year)
    last_year = (df[year_col] == dtt.today().year-1)

    # build new columns with sum/cumsum per year/grp
    df = (df
          .pipe(calc_col_partitioned, f'{prefix}sumJahr', fun='sum',    on=data_col, part_by=[year_col, grp_col])
          .pipe(calc_col_partitioned, f'{prefix}cumJahr', fun='cumsum', on=data_col, part_by=[year_col, grp_col]))

    # replace this year's sums with last year's
    last_year_sum_map = df.loc[last_year].groupby(grp_col)[data_col].agg('sum')
    df.loc[this_year, f'{prefix}sumJahr'] = df.loc[this_year, grp_col].apply(lambda x: last_year_sum_map[x])

    # divide cumsum by sum to get sum curve [0, 1], show in %
    df = (df.eval(f'{prefix}crvJahr = ({prefix}cumJahr / {prefix}sumJahr) * 100 + 0.5')
            .fillna({f'{prefix}crvJahr': 0})
            .astype({f'{prefix}crvJahr': 'int'}))
    return df
```

#### Auf Aggregationen

```python
pv_by_week = (pv_data
              .pipe(make_year_grp_sumcurve, year_col='JAHR', grp_col='PV_NR', data_col='RES_NETTO_NETTO', prefix='res_')
              .pipe(make_year_grp_sumcurve, year_col='JAHR', grp_col='PV_NR', data_col='AUS_NETTO_NETTO', prefix='aus_'))
```

```python
qgrid.show_grid(pv_by_week.loc[pv_by_week.PV_NR==20199,'PV_NR JAHR KW aus_sumJahr aus_cumJahr aus_cumJahr aus_crvJahr'.split()])
```

```python
desc_col(pv_by_week, det=True)
```

# Buchungsverlauf graphisch zeigen


* Für x-Achse: Jahr mit Nachkommastellen
* Für Gruppierung: PV_NR als String (Bokeh braucht das so für factor_cmap)
* Summenkurven hinzufügen

```python
pv_data['JahrKw'] = pv_data.JAHR.astype('float') + (pv_data.KW.astype('float') - 1) / 53
pv_data = (pv_data
           .pipe(make_year_grp_sumcurve, year_col='JAHR', grp_col='PV_NR', data_col='RES_NETTO_NETTO', prefix='res_')
           .pipe(make_year_grp_sumcurve, year_col='JAHR', grp_col='PV_NR', data_col='AUS_NETTO_NETTO', prefix='aus_')
           .pipe(as_dtype, 'str', incl_col='PV_NR'))
```

```python
def select_Pv(df, PvNr):
    labels = tuple(map(str, flatten(PvNr)))
    row_mask = df.PV_NR.isin(labels)
    return df.loc[row_mask]
```

#### Alle Buchungen

```python
import bokeh
from bokeh.plotting import figure, show
from bokeh.io import output_notebook
from bokeh.transform import factor_cmap

output_notebook()

pv_sel = pv_liste[:10]
source_data = select_Pv(pv_data, pv_sel)

p = figure(title=f"Buchungen pro Vertrag über Aushang", x_axis_label='Datum', y_axis_label='Netto', 
           plot_width=1000, tooltips=[("Vertrag", "@PV_NR")])
p.circle(x='JahrKw', y='AUS_NETTO_NETTO', source=source_data, size=6, alpha=0.3,
         color=factor_cmap('PV_NR', 'Category20_10', source_data.PV_NR.unique()),
         legend='PV_NR')
show(p)

p = figure(title=f"Buchungen pro Vertrag über Reservation", x_axis_label='Datum', y_axis_label='Netto', 
           plot_width=1000, tooltips=[("Vertrag", "@PV_NR")])
p.circle(x='JahrKw', y='RES_NETTO_NETTO', source=source_data, size=6, alpha=0.3,
         color=factor_cmap('PV_NR', 'Category20_10', source_data.PV_NR.unique()),
         legend='PV_NR')
show(p)
```

#### Jahresverlauf vergleichen
Verträge: Top 20 ohne SBB

```python
def graph_jahresverlauf(PvNr, typ='aushang'):
    """Jahres-Buchungsverlauf zeigen. PvNr kann >=1 PvNr enthalten, typ in ('aushang', 'reservation') oder Abk."""
    import altair as alt
    alt.data_transformers.enable('default', max_rows=None)

    data = select_Pv(pv_data, PvNr)
    data = (data.loc[(data.JAHR >= '2015') & (data.JAHR <= '2019')]
            .pipe(clean_up_categoricals, incl_col='JAHR'))
    
    if typ[:3] == 'res':
        prefix = 'res_'
    elif typ[:3] == 'aus':
        prefix = 'aus_'
    else:
        raise ValueError("typ in ('aushang', 'reservation') oder abgekürzt")

    pv_select = alt.selection_multi(fields=['PV_NR'], nearest=True)
    pv_color = alt.condition(pv_select,
                             alt.Color('PV_NR:N', legend=None),
                             alt.value('lightgray'))

    yr_select = alt.selection_multi(fields=['JAHR'])
    yr_color = alt.condition(yr_select,
                             alt.value('black'),
                             alt.value('lightgray'))

    # X axis: no auto-scaling per category
    kw_axis = alt.X('KW', scale=alt.Scale(rangeStep=None))

    # line graphs
    lines = alt.Chart(data).mark_line(strokeWidth=3, interpolate='linear').encode(
        x=kw_axis,
        color=pv_color,
        opacity=alt.Opacity('JAHR', legend=None),
        tooltip=['KW', 'JAHR', f'{prefix}cumJahr', f'{prefix}crvJahr']
    ).add_selection(
        pv_select
    ).transform_filter(
        pv_select
    ).transform_filter(
        yr_select
    )
    
    lines_cum = lines.encode(y=f'{prefix}cumJahr')
    lines_crv = lines.encode(y=f'{prefix}crvJahr')

    # clickable Pv legend
    pv_legend = alt.Chart(data).mark_rect().encode(
        y=alt.Y('PV_NR:N', sort=PvNr, axis=alt.Axis(orient='right')),
        color=pv_color
    ).add_selection(
        pv_select
    )

    # clickable AJahr legend
    yr_legend = alt.Chart(data).mark_circle(size=150).encode(
        y=alt.Y('JAHR:N', axis=alt.Axis(orient='right')),
        color=yr_color,
        opacity=alt.condition(yr_select,
                              alt.Opacity('JAHR:N', legend=None),
                              alt.value(0.25))
    ).add_selection(
        yr_select
    )

    # lay out graphs
    return ((lines_cum | (pv_legend | yr_legend)) & 
            (lines_crv | (pv_legend | yr_legend))
           ).configure_view(height=400, width=700)
```

```python
graph_jahresverlauf(pv_liste[:20], 'reserv').display()
```

# Aushang per Vertrag per Datum
Mit Vergleich zu Vorjahren (gleiche KW)

```python
def aushang(date):
    (jahr, kw) = date.isocalendar()[:2]
    
    result_columns = ['PvNr', 'Jahr', 'aus_sumJahr', 'aus_cumJahr', 'aus_crvJahr']
    result_labels  = ['PvNr', 'Jahr', 'total', 'cum', 'prc']
    tab = (pv_by_week.query('Kw == @kw')
                     .loc[:,result_columns]
                     .rename(columns=dict(zip(result_columns, result_labels)))
                     .pivot(index='PvNr', columns='Jahr', values=['total', 'cum', 'prc']))
    cols = [f'{lbl}_{yr}' for (lbl, yr) in tab.columns.to_flat_index()]
    tab.set_axis(labels=cols, axis='columns', inplace=True)
    return tab
```

### Aushang bis heute

```python
aus_per_heute = aushang(dtt.today())

alle_pv = aus_per_heute.loc[:,['total_2018', 'cum_2019', 'cum_2018', 'cum_2017']].sum(axis=0).astype('int')
print(f'Über alle Verträge:\n{alle_pv}')

aus_per_heute.query('total_2018 > 20000')
```

<!-- #region {"toc-hr-collapsed": false} -->
### Bereits gebuchter Aushang bis Ende Jahr

**Achtung:** Der 28. Dezember ist gemäss ISO-Logik der letzte Tag, der sicher noch in diesem Geschäftsjahr liegt
<!-- #endregion -->

```python
aus_per_ende_jahr = aushang(dtt(dtt.today().year, month=12, day=28))

alle_pv = aus_per_ende_jahr.loc[:,['total_2018', 'cum_2019']].sum(axis=0).astype('int')
print(f'Über alle Verträge:\n{alle_pv}')

aus_per_ende_jahr.query('total_2018 > 20000')
```

# Reservation per Vertrag per Datum
Mit Vergleich zu Vorjahren (gleiche KW)

```python
def reservation(date):
    (jahr, kw) = date.isocalendar()[:2]
    
    result_columns = ['PvNr', 'Jahr', 'res_sumJahr', 'res_cumJahr', 'res_crvJahr']
    result_labels  = ['PvNr', 'Jahr', 'total', 'cum', 'prc']
    tab = (pv_by_week.query('Kw == @kw')
                     .loc[:,result_columns]
                     .rename(columns=dict(zip(result_columns, result_labels)))
                     .pivot(index='PvNr', columns='Jahr', values=['total', 'cum', 'prc']))
    cols = [f'{lbl}_{yr}' for (lbl, yr) in tab.columns.to_flat_index()]
    tab.set_axis(labels=cols, axis='columns', inplace=True)
    return tab
```

### Reservation bis heute

```python
res_per_heute = reservation(dtt.today())

alle_pv = res_per_heute.loc[:,['total_2018', 'cum_2019', 'cum_2018', 'cum_2017']].sum(axis=0).astype('int')
print(f'Über alle Verträge:\n{alle_pv}')

res_per_heute
```

### Berechne prozentuelle Abweichung auf tiefstes Vorjahr, Betragsabweichung (skaliert auf Vorjahr)

```python
def make_diff_cols(df):
    df = (df.assign(prc_diff = df.prc_2019 - np.minimum(df.prc_2017, df.prc_2018))
            .eval('cum_diff = prc_diff/100 * total_2018'))
    return df
```

```python
r = make_diff_cols(res_per_heute)
```

```python
r[['total_2018', 'prc_diff', 'cum_diff']].describe()
```

```python
r.reset_index(inplace=True)
```

```python
r = r.assign(Partner = r.PvNr.apply(lambda x: pv_info.at[x,'Partner']), 
             Titel   = r.PvNr.apply(lambda x: pv_info.at[x,'Titel']),
             Total   = r.total_2017 + r.total_2018 + r.total_2019)

pv_output = (pd.DataFrame(dict(Vertrag=r.PvNr, Partner=r.Partner, Titel=r.Titel, Total_2017=r.total_2017, Total_2018 = r.total_2018,
                               Stand_2017=r.cum_2017, Stand_2018=r.cum_2018, Stand_2019=r.cum_2019, Diff=r.cum_diff))
             .sort_values('Diff')
             .reset_index(drop=True))

store_excel(pv_output, 'res_per_pv.xlsx')
```

### Plot: % Vorjahr vs. % dieses Jahr, Grösse ~ Umsatz Vorjahr

```python
import altair as alt

# Neue Spalte 'pv_size' = srqt(Umsatz 2018)
data = (r.query('total_2018 > 1000')
         .assign(pv_size=np.sqrt(r.total_2018))
         .reset_index().sort_values('total_2018'))

points = alt.Chart(data).mark_circle(clip=True).encode(
    x=alt.X('prc_2018'),
    y=alt.Y('prc_2019', scale=alt.Scale(domain=[0, 100])),
    size='pv_size',
    tooltip=['PvNr'],
    #color='pv_size'
)

# Norm-Diagonale
diag = alt.Chart(
    pd.DataFrame({'x': [0, 100], 'y': [0, 100]})
).mark_line(color='lightgray', strokeWidth=1).encode(x='x', y='y')

(diag + points).configure_view(width=600, height=600).interactive()
```
