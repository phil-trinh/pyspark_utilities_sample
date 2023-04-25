"""
Test the ensure_no_conflict function in parsing/non_dataframe_string_fun.py.

ensure_no_conflict(existing: list, column: str) -> str
"""

from ..unit_test_utils import assertion_helper

from transformations_library.parsing.non_dataframe_string_fun import ensure_no_conflict


def test_ensure_no_conflict_0() -> None:
    """
    Test the function: no conflicts, should not change input.
    """
    column_list = ["column1", "column2"]
    new_column = "column3"
    update = ensure_no_conflict(column_list, new_column)
    assertion_helper(new_column, update, f"{new_column} does not match {update} in test_0")


def test_ensure_no_conflict_1() -> None:
    """
    Test the function: multiple conflicts, should surround input with _'s.
    """
    column_list = ["column1", "column2", "_column2_", "__column2__", "___column2"]
    new_column = "column2"
    update = ensure_no_conflict(column_list, new_column)
    assertion_helper(update, "___column2___", f"{update} does not match ___column2___ in test_1")

