"""
Test the function return_year_month in date_handling/today.py

return_year_month() -> tuple
"""

import datetime

from ..unit_test_utils import assertion_helper

from transformations_library.date_handling.today import return_year_month


def test_return_year_month_0():
    """
    Test the function.
    """
    # Calculate the result for today so that whenever these checks run, this will pass.
    today = datetime.datetime.today()
    (year, month) = return_year_month()
    assertion_helper(today.year, year, "Year mismatch")
    assertion_helper(today.month, month, "Month mismatch")

