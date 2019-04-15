#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Writing to PA log file and STDOUT

@author: kpf
"""

import time
import logging as lg
from logging.handlers import WatchedFileHandler
from contextlib import ContextDecorator

from pa_lib.const import PA_LOG_DIR

def _init():
    log_path = PA_LOG_DIR
    log_file = 'pa_log.txt'

    logger = lg.getLogger('pa_log')
    logger.setLevel(lg.INFO)

    # # File handler
    # file_hdl = WatchedFileHandler(log_path + log_file)
    # file_hdl.setLevel(lg.INFO)

    # Stream handler (writes to STDERR)
    stream_hdl = lg.StreamHandler()
    stream_hdl.setLevel(lg.INFO)

    # Define format, add to file handler
    log_fmt = lg.Formatter(fmt='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # file_hdl.setFormatter(log_fmt)
    stream_hdl.setFormatter(log_fmt)

    # Add handlers (only once)
    if not logger.hasHandlers():
        # logger.addHandler(file_hdl)
        logger.addHandler(stream_hdl)

    return logger

pa_logger = _init()

def info(msg):
    pa_logger.info(msg)

def warn(msg):
    pa_logger.warning(msg)

def err(msg):
    pa_logger.error(msg)

class time_log(ContextDecorator):
    def __init__(self, name):
        self.name       = name
        self.start_time = None
        self.start_cpu  = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        self.start_cpu  = time.process_time()

    def __exit__(self, exc_type, exc, exc_tb):
        elapsed = time.perf_counter() - self.start_time
        cpu     = time.process_time() - self.start_cpu
        info(f'Finished {self.name} in {round(elapsed, 2)}s ({round(cpu, 2)}s CPU)')


###############################################################################
# TESTING CODE
###############################################################################

if __name__ == "__main__":
    info('-- Testing pa_log module - info')
    warn('-- Testing pa_log module - warn')
    err ('-- Testing pa_log module - err')

    @time_log('decorated function')
    def test_function():
        time.sleep(0.2)
    test_function()

    with time_log('timed context'):
        time.sleep(0.3)
