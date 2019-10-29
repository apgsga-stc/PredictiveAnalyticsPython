#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Writing to PA log file and STDOUT

@author: kpf
"""

# Imports
import time
import sys
from contextlib import ContextDecorator
from pa_lib.const import PA_LOG_DIR

# Globals
_log_file_path = ""
_use_log_file = False


########################################################################################
def set_log_file(file_name):
    """
    Activate file logging into file 'file_name' in directory PA_LOG_DIR.
    Set to None to deactivate file logging.
    """
    global _log_file_path, _use_log_file
    if file_name is not None:
        _log_file_path = PA_LOG_DIR / file_name
        _use_log_file = True
        PA_LOG_DIR.mkdir(exist_ok=True, parents=True)
        _log_file_path.touch()

    else:
        _use_log_file = False


########################################################################################
def _log(msg):
    if _use_log_file:
        with _log_file_path.open(mode="a") as log_file:
            print(msg, file=log_file)
    print(msg, file=sys.stderr)


def _format(msg, level):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp} [{level.upper()}] {msg}"


def info(msg):
    _log(_format(msg, level="INFO"))


def warn(msg):
    _log(_format(msg, level="WARN"))


def err(msg):
    _log(_format(msg, level="ERROR"))


########################################################################################
class time_log(ContextDecorator):
    def __init__(self, name):
        self.name = name
        self.start_time = None
        self.start_cpu = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        self.start_cpu = time.process_time()
        info(f"Started {self.name}")

    def __exit__(self, exc_type, exc, exc_tb):
        elapsed = time.perf_counter() - self.start_time
        cpu = time.process_time() - self.start_cpu
        info(f"Finished {self.name} in {round(elapsed, 2)}s ({round(cpu, 2)}s CPU)")


########################################################################################
# TESTING CODE
########################################################################################
if __name__ == "__main__":
    info("-- Testing pa_log module - info")
    warn("-- Testing pa_log module - warn")
    err("-- Testing pa_log module - err")

    @time_log("decorated function")
    def _test_function():
        time.sleep(0.2)

    _test_function()

    with time_log("timed context"):
        time.sleep(0.3)
