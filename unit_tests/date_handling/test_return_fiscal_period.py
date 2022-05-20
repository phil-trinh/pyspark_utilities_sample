"""
Test the function return_fiscal_period in date_handling/today.py

return_fiscal_period() -> tuple
"""

import datetime

from ..unit_test_utils import assertion_helper

from transformations_library.date_handling.today import return_fiscal_period


def test_return_fiscal_period():
    """
    Test the function.
    """
    # Calculate the result for today so that whenever these checks run, this will pass.
    today = datetime.datetime.today()
    f_year = today.year + 1 if today.month >= 10 else today.year
    f_month = today.month - 9 if today.month >= 10 else today.month + 3
    (year, month) = return_fiscal_period()
    assertion_helper(f_year, year, "Year mismatch")
    assertion_helper(f_month, month, "Month mismatch")
