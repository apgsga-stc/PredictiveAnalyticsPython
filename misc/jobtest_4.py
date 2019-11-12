# make imports from pa_lib possible (parent directory of file's directory)
try:
    import sys
    from pathlib import Path

    _file_dir = Path.cwd()
    _parent_dir = _file_dir.parent
    sys.path.append(str(_parent_dir))

    import pandas as pd
    from pa_lib.file import project_dir, store_csv
    from pa_lib.log import info, err

    info("Starting jobtest_4.py!")

    df = pd.DataFrame(data=dict(nr=[1, 2, 3, 4], txt=list("abcd")))
    with project_dir("jobtest"):
        store_csv(df, "jobtest_4.csv", do_zip=False)
except:
    err(f"Exception caught: {sys.exc_info()[1]}")

info("Ending jobtest_4.py")
