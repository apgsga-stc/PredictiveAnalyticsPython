# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

import json
from pa_lib.const import PA_JOB_DIR
from pa_lib.log import info, err

# Global variables
job_data_dir = PA_JOB_DIR
job_struct_file_path = job_data_dir / "job_struct.json"
job_attributes = ["script_dir", "script", "project_dir", "result"]


def load_job_struct():
    # read and validate file
    try:
        with job_struct_file_path.open() as job_struct_file:
            job_struct = json.load(job_struct_file)
    except FileNotFoundError:
        job_data_dir.mkdir(parents=True, exist_ok=True)
        job_struct = dict()
        with job_struct_file_path.open(mode="w") as job_struct_file:
            json.dump(job_struct, job_struct_file)
    except json.decoder.JSONDecodeError:
        err(f"JSON structure defect in file {job_struct_file_path}, exiting.")
        err(f"Error message: {sys.exc_info()[1]}")
        sys.exit(1)

    for job, attrs in job_struct.items():
        if list(attrs.keys()) != job_attributes:
            err(f"Illegal job attributes in file {job_struct_file_path}, exiting.")
            err(
                f"Job {job} must have attributes {job_attributes}, has {list(attrs.keys())}"
            )
            sys.exit(1)
        (script_dir, script, project_dir, result) = attrs.values()
        if not (parent_dir / script_dir / script).exists():
            err(f"Script file not found for job {job}, exiting.")
            err(
                f"Job script {script} not found in script directory {script_dir} under {parent_dir}."
            )
            sys.exit(1)

    return job_struct


job_tree = load_job_struct()
print(job_tree)
