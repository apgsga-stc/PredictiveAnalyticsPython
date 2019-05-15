from pathlib import Path
from pa_lib.file import file_list

sql_path = Path('pa_lib') / 'sql'

QUERY = {}
for sql_file in file_list(sql_path, '*.sql').name:
    tag = sql_file[0:-4]
    with open(sql_path / sql_file) as query_file:
        QUERY[tag] = query_file.read()