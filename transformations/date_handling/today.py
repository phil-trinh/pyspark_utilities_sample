"""
Table of Contents

1. return_year_month: Return the current year and month.
2. return_fiscal_period: Determine fiscal period from today's date.
"""

import datetime


def return_year_month() -> tuple:
    """
    Return the current year and month.

    Inputs
        None

    Output
        Tuple of (calendar year, calendar month) in integer form.

    Example
        (year, month) = return_year_month()
        Will return the year and month of today's date.
    """
    today = datetime.datetime.today()

    return (today.year, today.month)


def return_fiscal_period() -> tuple:
    """
    Determine fiscal period from today's date.

    Inputs
        None

    Output
        Tuple of (fiscal year, fiscal month) in integer form.

    Example
        (year, month) = return_fiscal_period()
        Will convert today's date into the fiscal period.
    """
    (year, month) = return_year_month()

    # Convert year first using calendar month.
    if month >= 10:
        year += 1

    # Convert calendar month to fiscal month.
    if month >= 10:
        month -= 9
    else:
        month += 3

    return (year, month)
