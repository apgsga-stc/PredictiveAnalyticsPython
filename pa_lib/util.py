#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 14:56:50 2019
PA utility functions:
    * Sequence joining
    * Object flattening
    * Storage size formatting
    * Object size calculation
    * ISO week calculation with rounding

@author: kpf
"""
import pandas as pd
import sys

from datetime import datetime as dtt
from itertools import chain
from collections import OrderedDict, deque


###############################################################################
def cap_words(txt, sep=None):
    """Capitalize each word of txt where 'words' are separated by sep"""
    return sep.join(map(lambda word: word.capitalize(), txt.split(sep)))


###############################################################################
def contents(obj):
    """Show attributes of an object with their types"""
    tab = pd.DataFrame({'name': dir(obj), 
                        'type': [type(obj.__getattribute__(attr)) 
                                 for attr in dir(obj)]})
    return tab

    
###############################################################################
def seq_join(seq, sep=' '):
    """Join seq to a string separated by sep. Seq can by any iterable"""
    return sep.join(map(str, seq))


###############################################################################
def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True


def is_string(obj):
    return isinstance(obj, (str, bytes))


def flatten(obj):
    """Flatten complex object recursively to linear sequence.
       Supports nesting levels up to the interpreter's recursion depth limit."""
    # iterate over sub-elements unless they're strings (infinite recursion!)
    if is_iterable(obj) and not is_string(obj):
        for element in obj:
            yield from flatten(element)
    else:
        yield obj


###############################################################################
def format_size(size):
    """Format "size" (in bytes) to human-readable string"""
    power = 1024
    labels = {0: '', 1: 'K', 2: 'M', 3: 'G'}
    n = 0
    while size >= power and n <= len(labels):
        size /= power
        n += 1
    return f'{round(size, 1)} {labels[n]}B'


###############################################################################
def _total_size(obj, handlers=OrderedDict()):
    """ Returns the approximate memory footprint an object
        and all of its contents.

        Automatically finds the contents of the following builtin containers and
        their subclasses:  tuple, list, deque, dict, set and frozenset.
        To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}
    """
    DFLT_SIZE = sys.getsizeof(0)

    def sizeof(my_obj):
        if id(my_obj) in seen:  # do not double count the same object
            return 0
        seen.add(id(my_obj))
        nbytes = sys.getsizeof(my_obj, DFLT_SIZE)
        for typ, handler in all_handlers.items():
            if isinstance(my_obj, typ):
                nbytes += sum(map(sizeof, handler(my_obj)))
                break
        return nbytes

    def dict_handler(d):
        return chain.from_iterable(d.items())
    
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter
                    }
    all_handlers.update(handlers)  # user handlers take precedence
    seen = set()  # track which object id's have already been seen
    return sizeof(obj)


def obj_size(obj, do_format=True):
    """Return memory usage of a python object."""
    if isinstance(obj, pd.DataFrame):
        nbytes = obj.memory_usage(deep=True).sum()
    elif isinstance(obj, pd.Series):
        nbytes = obj.memory_usage(deep=True)
    else:
        nbytes = _total_size(obj)
    if do_format:
        return format_size(nbytes)
    else:
        return nbytes


###############################################################################
def last_monday(date):
    """Monday of the 'date's week, midnight"""
    return pd.Timestamp(date).normalize() - pd.Timedelta(days=date.weekday())


def week(date):
    """Week nr of a date in native datetime format"""
    date = pd.to_datetime(date)
    return int(dtt.strftime(date, '%W'))


def week_txt(date_txt):
    """Normal week nr of a date in format 'YYYY-MM-DD'"""
    date = dtt.strptime(date_txt, '%Y-%m-%d')
    week_nr = week(date)
    return week_nr


def iso_week(date):
    """ISO week nr of a date in native datetime format"""
    date = pd.to_datetime(date)
    return int(dtt.strftime(date, '%V'))


def iso_week_txt(date_txt):
    """ISO week nr of a date in format 'YYYY-MM-DD'"""
    date = dtt.strptime(date_txt, '%Y-%m-%d')
    week_nr = iso_week(date)
    return week_nr


def _rd(nr, by):
    return int(nr / by) * by


def iso_week_rd(date, rd_period=2):
    """
    ISO week nr rounded to a period length, starting with odd weeks
    For 2 or 4 week-periods, week 53 is assigned to last complete period
    """
    week_nr = _rd(iso_week(date) - 1, rd_period) + 1
    if rd_period in (2, 4):
        return min(week_nr, 53 - rd_period)
    else:
        return week_nr


def iso_year(date):
    return int(dtt.strftime(date, '%G'))


###############################################################################
# TESTING CODE
###############################################################################

if __name__ == "__main__":
    print(seq_join((f"{nbytes} bytes are {format_size(nbytes)}" for nbytes in
                    [1024, 123456, 123456789]), sep='\n'))

    testlist = [range(4), ['abc', 'd', ('e', 'f')], 1, (2, 3)]
    print(testlist)
    print(list(flatten(testlist)))
    print(list(flatten(None)))
    print(f"Today is in ISO week {iso_week(dtt.today())}")
    print(f"5.3.2019 should have week nr 9: {week_txt('2019-03-05')}")
    print(f"5.3.2019 should have ISO week nr 10: {iso_week_txt('2019-03-05')}")

    year_days = pd.date_range(dtt(2017, 1, 1), dtt(2017, 12, 31))
    testtab = pd.DataFrame.from_records(
        columns=['date', 'kw', 'iso', 'iso2', 'iso4'],
        data=[(d, week(d), iso_week(d), iso_week_rd(d, 2), iso_week_rd(d, 4))
              for d in year_days],
        index='date'
    )
    print(testtab)
    print(f'Size of testtab  : {obj_size(testtab)}')
    print(f"Size of 'testtab': {obj_size('testtab')}")
    print(f"Size of None     : {obj_size(None)}")
