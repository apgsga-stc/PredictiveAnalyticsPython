#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 12:01:34 2019

@author: kpf
"""

import sys
import pandas as pd
from datetime import datetime as dtt
from dataclasses import dataclass, astuple

if '/home/pa/python/' not in sys.path:
    sys.path.insert(0, '/home/pa/python/')
from pa_lib.util import format_size


dtFactor = pd.api.types.CategoricalDtype(ordered=True)
dtYear   = pd.api.types.CategoricalDtype(ordered=True)
dtKW     = pd.api.types.CategoricalDtype(categories = [w+1 for w in range(53)], 
                                         ordered    = True)

def as_years(col):
    return col.astype('int').astype(dtYear)

def as_kw(col):
    return col.astype('int').astype(dtKW)

def merge_years(col1, col2):
    merged = pd.api.types.union_categoricals(to_union        = [col1.astype(dtYear), col2.astype(dtYear)], 
                                             ignore_order    = True,
                                             sort_categories = True)
    return merged.as_ordered()

@dataclass
class ConnectPar:
    """Holds Oracle Connection parameters"""
    instance: str
    user:     str
    passwd:   str
    
    def astuple(self):
        return astuple(self)


@dataclass
class File:
    name: str
    size: int
    mtime: dtt
    
    def __str__(self):
        return f'"{self.name}" ({format_size(self.size)}), last modified {dtt.strftime(self.mtime, "%Y-%m-%d %H:%M:%S")}'

    
class Record(object):
    """Simple container class, allows both attribute and dict item access"""
    def __init__(self, *args, **kwargs):
        """Initialize from keyword parameters"""
        for (key, value) in kwargs.items():
            setattr(self, key, value)
            
    def __repr__(self):
        attr_repr = ', '.join(f'{k}={repr(v)}' for (k, v) in self.items())
        return f'Record({attr_repr})'
    
    def __getitem__(self, key):
        """Get attribute like from a dict"""
        return getattr(self, key)
        
    def __setitem__(self, key, value):
        """Set attribute like on a dict"""
        setattr(self, key, value)
        
    def __iter__(self):
        return iter(self.__dict__)
    
    def __len__(self):
        return len(self.__dict__)
    
    def keys(self):
        return list(self.__dict__.keys())
    
    def values(self):
        return list(self.__dict__.values())
    
    def items(self):
        return list(self.__dict__.items())
    
