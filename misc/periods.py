# make imports from pa_lib possible (parent directory of file's directory)
import sys
from pathlib import Path

file_dir = Path.cwd()
parent_dir = file_dir.parent
sys.path.append(str(parent_dir))

from pa_lib.util import iso_week_rd


def iso_year(date):
    return date.isocalendar()[0]


def period_diff(year1, period1, year2, period2, rd=2):
    diff = (year2 - year1) * (52 // rd)
    diff += period2 - period1
    return diff


def make_period_diff(df, year_col_1, period_col_1, year_col_2, period_col_2, diff_col='diff', rd=2):
    return df.eval(f'{diff_col} = ({year_col_2} - {year_col_1}) * (52 // {rd}) \
                                + ({period_col_2} - {period_col_1})')


class BookPeriod:

    def __init__(self, date=None, year=None, period=None, rd_period=2):
        if date is not None:
            self.year = iso_year(date)
            self.period = iso_week_rd(date, rd_period=rd_period)
        if year is not None and period is not None:
            (self.year, self.period) = (year, period)
