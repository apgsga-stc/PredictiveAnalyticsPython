import subprocess as sp
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from pandarallel import pandarallel

from pa_lib import file, log, data


# Read command output
with sp.Popen('ls -l', shell=True, stdout=sp.PIPE) as p:
    output = b''.join(p.stdout).decode()

df1 = pd.DataFrame(dict(nr1=range(10), nr2=range(1,11)))

# Write dataframe through pipe
with sp.Popen(['zip daten.csv.zip -'], shell=True, stdin=sp.PIPE, universal_newlines=True) as p:
    df1.to_csv(p.stdin)

# Read dataframe through pipe
with sp.Popen('7za e -so daten.csv.zip', shell=True, stdout=sp.PIPE) as p:
    df2 = pd.read_csv(p.stdout, encoding='cp1252', dtype=np.str)


# Group aggregation using multiple processes
file.data_files()
bd = file.load_bin('bd_data_vkprog.feather')
bd = bd.loc[:,'ENDKUNDE_NR NETTO KAMP_ERFASS_JAHR KAMP_ERFASS_KW_2 KAMP_BEGINN_JAHR KAMP_BEGINN_KW_2'.split()]

pandarallel.initialize(nb_workers=8, progress_bar=True)

# 111s. This results in a series of dataframes, indexed by ENDKUNDE_NR. Needs concatenating?
def ek_sum(df):
    return df.groupby(['KAMP_ERFASS_JAHR', 'KAMP_ERFASS_KW_2'], observed=False, as_index=False)[['NETTO']].agg('sum')
with log.time_log('calculating sums'):
    tmp = bd.groupby('ENDKUNDE_NR', as_index=True).parallel_apply(ek_sum)

# 348s
    with log.time_log('calculating sums'):
    tmp = bd.groupby(['ENDKUNDE_NR', 'KAMP_ERFASS_JAHR', 'KAMP_ERFASS_KW_2'], observed=False, as_index=False)[['NETTO']].agg('sum')
