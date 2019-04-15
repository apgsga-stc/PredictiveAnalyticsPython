import subprocess as sp
import pandas as pd

# Read command output
with sp.Popen('ls -l', shell=True, stdout=sp.PIPE) as p: 
    output = b''.join(p.stdout).decode()

# Read dataframe from pipe
with sp.Popen('7za e -so daten.csv.zip', shell=True, stdout=sp.PIPE) as p:
    bd = pd.read_csv(p.stdout, encoding='cp1252', dtype=np.str) 
