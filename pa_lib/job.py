# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

_file_dir = Path.cwd()
_parent_dir = _file_dir.parent
sys.path.append(str(_parent_dir))

# Imports
import json
import subprocess
from datetime import datetime as dtt
from os import path
from pa_lib.const import PA_JOB_DIR, PA_DATA_DIR
from pa_lib.log import info, err, set_log_file

# Global variables
_job_data_dir = PA_JOB_DIR
_job_struct_file_path = _parent_dir / "pa_lib" / "jobs" / "job_struct.json"
_job_attributes = ["script_dir", "project_dir", "result"]


########################################################################################
def _load_job_struct():
    """
    Read job structure file, validate structure.
    """
    # Read JSON file
    try:
        with _job_struct_file_path.open() as job_struct_file:
            job_struct = json.load(job_struct_file)
    except FileNotFoundError:
        _job_data_dir.mkdir(parents=True, exist_ok=True)
        job_struct = dict()
        with _job_struct_file_path.open(mode="w") as job_struct_file:
            json.dump(job_struct, job_struct_file)
    except json.decoder.JSONDecodeError:
        err(f"JSON structure defect in file {_job_struct_file_path}, exiting.")
        err(f"--> Error message: {sys.exc_info()[1]}")
        sys.exit(1)

    # Validate job structure
    for job, attrs in job_struct.items():
        if list(attrs.keys()) != _job_attributes:
            err(f"Illegal job attributes in file {_job_struct_file_path}, exiting.")
            err(
                f"--> Job '{job}' must have attributes {_job_attributes}, has {list(attrs.keys())}"
            )
            sys.exit(1)
        (script_dir, project_dir, result) = attrs.values()
        if not (_parent_dir / script_dir / job).exists():
            err(
                f"{job} not found in script directory {script_dir} under {_parent_dir}', exiting."
            )
            sys.exit(1)
        job_struct[job]["script_path"] = str(_parent_dir / attrs["script_dir"] / job)

    return job_struct


########################################################################################
def _check_job_name(job_name):
    if job_name not in _job_tree:
        err(f"Job '{job_name}' is not defined in {_job_struct_file_path}, exiting.")
        err(f"--> List of defined jobs: {list(_job_tree)}")
        sys.exit(1)


########################################################################################
def _job_last_run_timestamp(job_name):
    job = _job_tree[job_name]
    result_path = PA_DATA_DIR / job["project_dir"] / job["result"]
    if not result_path.exists():
        return None
    return dtt.fromtimestamp(result_path.stat().st_mtime)


########################################################################################
def _run_is_current(run_timestamp, current):
    if run_timestamp is None:
        return False

    def iso_year_week(date):
        return dtt.isocalendar(date)[:2]

    if current == "Today":
        return run_timestamp.date() == dtt.today().date()
    elif current == "This Week":
        return iso_year_week(run_timestamp) == iso_year_week(dtt.today())
    else:
        err(f"Wrong parameter value for 'current': '{current}', exiting.")
        err("--> Parameter must be in ['Today', 'This Week']")
        sys.exit(1)


########################################################################################
def _run_job(job_name):
    """
    Run job 'job_name', capture all output.
    Returns (success, stdout, stderr).
    """
    result = subprocess.run(
        ["python", _job_tree[job_name]["script_path"]], capture_output=True, text=True
    )
    return (
        (result.returncode == 0),
        result.stdout.rstrip("\n"),
        result.stderr.rstrip("\n"),
    )


########################################################################################
def request_job(job_name, current=None, show_stdout=False):
    """
    Make sure that job 'job_name' has been run recently.
    'current' can be set to any of ["Today", "This Week"], or to None (default)
    If set to None, the job is started immediately.
    In case of job failure, STDERR from the job is shown.
    In case of job success, STDOUT from the job is only shown if show_stdout=True.
    """
    _check_job_name(job_name)
    this_script = path.basename(sys.argv[0])
    info(f"[{this_script}] requests '{job_name}'")
    last_run_timestamp = _job_last_run_timestamp(job_name)
    run_reason = ""
    if current is None:
        run_reason = "Unconditional request"
    elif last_run_timestamp is None:
        run_reason = "No previous run result found"
    elif not _run_is_current(last_run_timestamp, current):
        run_reason = f"Run result is out of date ('{current}')"

    if run_reason:
        info(f"[{this_script}]: Running job '{job_name}': {run_reason}")
        (success, stdout, stderr) = _run_job(job_name)
        if success:
            if show_stdout:
                for line in stdout.splitlines():
                    info(f"[{job_name}]: {line}")
            info(f"[{this_script}]: Job '{job_name}' finished successfully!")
        else:
            for line in stderr.splitlines():
                err(f"{job_name} - {line}")
            err(
                f"[{this_script}]: Error requesting job '{job_name}': Job failed, exiting."
            )
            sys.exit(1)
    else:
        info(
            f"[{this_script}]: Not running job '{job_name}': result is current ('{current}') from {last_run_timestamp}."
        )


########################################################################################
# MAIN CODE
########################################################################################
set_log_file(f"{dtt.today().strftime('%Y%m%d')}_pa_job_log.txt")
_job_tree = _load_job_struct()


###############################################################################
# TESTING CODE
###############################################################################
if __name__ == "__main__":
    request_job("jobtest_1.py", current="Today")
