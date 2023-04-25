"""
Test the clean_name_for_header function in parsing/non_dataframe_string_fun.py.

clean_name_for_header(value: str, as_lower: bool = False, as_upper: bool = False) -> str
"""

from ..unit_test_utils import assertion_helper

from transformations_library.parsing.non_dataframe_string_fun import clean_name_for_header

STANDARD_MESSAGE = "Expected {0}, computed {1}; {2}"


def test_clean_name_for_header_0():
    """
    Test the function: as_lower=True.
    """
    output = clean_name_for_header("(Not) A=Good.Name", as_lower=True)
    correct = "_not__a_good_name"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_0"))


def test_clean_name_for_header_1():
    """
    Test the function: as_upper=True.
    """
    output = clean_name_for_header("Not,A Good;Name{}", as_upper=True)
    correct = "NOT_A_GOOD_NAME__"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_1"))


def test_clean_name_for_header_2():
    """
    Test the function: as_lower=True and as_upper=True, lower takes precedence.
    """
    output = clean_name_for_header("Not A Good Name", as_lower=True, as_upper=True)
    correct = "not_a_good_name"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_2"))


def test_clean_name_for_header_3():
    """
    Test the function: as_lower=False and as_upper=False, case should not be changed.
    """
    value = """
    Not A
    Good
    Name
    """
    output = clean_name_for_header(value, as_lower=False, as_upper=False)
    correct = "_____Not_A_____Good_____Name_____"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_3"))


def test_clean_name_for_header_4():
    """
    Test the function: as_lower=False and as_upper=False, case should not be changed, collapse _'s.
    """
    value = "[](Not) A===Good.Name "
    output = clean_name_for_header(value, collapse=True)
    correct = "_Not_A_Good_Name_"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_4"))


def test_clean_name_for_header_5():
    """
    Test the function: as_upper=True, trim=True, full set of characters to remove at end of string to be trimmed.
    """
    value = "(Not) A=Good.Name~!@#$%^&*+\\'\" ,;:[]{}()\n\t=."
    output = clean_name_for_header(value, as_upper=True, trim=True)
    correct = "NOT__A_GOOD_NAME"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_5"))


def test_clean_name_for_header_6():
    """
    Test the function: as_upper=True, compare and contrast collapse and trim True against both False.
    """
    value = "(Not) A=Good.Name {};:"

    output = clean_name_for_header(value, as_upper=True)
    correct = "_NOT__A_GOOD_NAME_____"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_6a"))

    output = clean_name_for_header(value, as_upper=True, collapse=True, trim=True)
    correct = "NOT_A_GOOD_NAME"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output, "test_6b"))

