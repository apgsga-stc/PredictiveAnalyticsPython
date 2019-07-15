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

## Parallelization

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

## Contingency Tables

```python
import numpy as np
import pandas as pd
```

```python
import statsmodels.api as sm
```

```python
df = sm.datasets.get_rdataset("Arthritis", "vcd").data
```

```python
df.dtypes
```

```python
df.head()
```

```python
tab = pd.crosstab(df['Treatment'], df['Improved'])
```

```python
tab
```

```python
tab = tab.loc[:, ["None", "Some", "Marked"]]
table = sm.stats.Table(tab)
```

```python
data = df[["Treatment", "Improved"]]
table = sm.stats.Table.from_data(data)
```

```python
display(table.table_orig)
display(table.fittedvalues)
display(table.resid_pearson)
```

```python
from statsmodels.graphics.mosaicplot import mosaic
fig,_ = mosaic(data, index=["Treatment", "Improved"], statistic=True, gap=0.01)
```

```python
print(table.test_nominal_association())
```

```python
print(table.test_ordinal_association())
```

## XLSX Writer

```python
%load_ext autoreload
%autoreload

from pa_lib.file import write_xlsx as xlsw
```

```python
df = pd.DataFrame({'Text': 'a b c d e f g h i j l k p o i u z t r e w q a s d f g h j k l m n b'.split(), 'Nr': list(range(34))})
```

```python
xlsw(df, 'testfile.xlsx')
```

## Facets

```python
from IPython.core.display import display, HTML
```

```python
def dive(df):
    jsonstr = df.to_json(orient='records')
    html = f"""
            <script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"></script>
            <link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/master/facets-dist/facets-jupyter.html">
            <facets-dive id="elem" height="800"></facets-dive>
            <script>
              var data = {jsonstr};
              document.querySelector("#elem").data = data;
            </script>"""
    display(HTML(html))
```

```python
dive(df)
```

### Clustering

```python
import numpy as np

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler


# #############################################################################
# Generate sample data
centers = [[1, 1], [-1, -1], [1, -1]]
X, labels_true = make_blobs(n_samples=750, centers=centers, cluster_std=0.4,
                            random_state=0)

X = StandardScaler().fit_transform(X)

# #############################################################################
# Compute DBSCAN
db = DBSCAN(eps=0.3, min_samples=10).fit(X)
core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
core_samples_mask[db.core_sample_indices_] = True
labels = db.labels_

# Number of clusters in labels, ignoring noise if present.
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
n_noise_ = list(labels).count(-1)

print('Estimated number of clusters: %d' % n_clusters_)
print('Estimated number of noise points: %d' % n_noise_)
print("Homogeneity: %0.3f" % metrics.homogeneity_score(labels_true, labels))
print("Completeness: %0.3f" % metrics.completeness_score(labels_true, labels))
print("V-measure: %0.3f" % metrics.v_measure_score(labels_true, labels))
print("Adjusted Rand Index: %0.3f"
      % metrics.adjusted_rand_score(labels_true, labels))
print("Adjusted Mutual Information: %0.3f"
      % metrics.adjusted_mutual_info_score(labels_true, labels,
                                           average_method='arithmetic'))
print("Silhouette Coefficient: %0.3f"
      % metrics.silhouette_score(X, labels))

# #############################################################################
# Plot result
%matplotlib inline
import matplotlib.pyplot as plt

# Black removed and is used for noise instead.
unique_labels = set(labels)
colors = [plt.cm.Spectral(each)
          for each in np.linspace(0, 1, len(unique_labels))]
for k, col in zip(unique_labels, colors):
    if k == -1:
        # Black used for noise.
        col = [0, 0, 0, 1]

    class_member_mask = (labels == k)

    xy = X[class_member_mask & core_samples_mask]
    plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),
             markeredgecolor='k', markersize=14)

    xy = X[class_member_mask & ~core_samples_mask]
    plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),
             markeredgecolor='k', markersize=6)

plt.title('Estimated number of clusters: %d' % n_clusters_)
plt.show()
```

## Sending E-Mail

```python
from smtplib import SMTP

from email.message import EmailMessage
from email.headerregistry import Address
```

#### Talk to me, please!

```python
with SMTP(host='mailint.apgsga.ch') as mail_gateway:
    mail_gateway.set_debuglevel(True)
    mail_gateway.noop()
    mail_gateway.helo('http://lxewi041.apgsga.ch')
```

```python
with SMTP(host='mailint.apgsga.ch') as mail_gateway:
    mail_gateway.set_debuglevel(True)
    mail_gateway.sendmail(from_addr='predictive_analytics.apgsga.ch', 
                          to_addrs='kaspar.pflugshaupt@apgsga.ch', 
                          msg='This is a test message')
```

```python
msg = EmailMessage()
msg['Subject'] = "Testing sendmail"
msg['From'] = Address("Predictive Analytics", "predictive_analytics", "apgsga.ch")
msg['To'] = (Address("Kaspar Pflugshaupt", "kaspar.pflugshaupt", "apgsga.ch")) #,
             #Address("Sam Truong", "sam.truong", "apgsga.ch"))
msg.set_content("""\
Das ist ein Test:

P:\Service\Kennzahlen\Verkauf\PredictiveAnalytics
""")

# Add the html version.  This converts the message into a multipart/alternative
# container, with the original text message as the first part and the new html
# message as the second part.
msg.add_alternative("""\
<html>
  <head></head>
  <body>
    <p>Das ist ein Test:</p>
    <a href="P:\Service\Kennzahlen\Verkauf\PredictiveAnalytics">Verzeichnis</a>
  </body>
</html>
""", subtype='html')
```

```python
print(msg.as_string())
```

```python
with SMTP(host='mailint.apgsga.ch') as mail_gateway:
    mail_gateway.set_debuglevel(True)
    mail_gateway.send_message(msg)
```
