"""
Test the camel_to_snake function in parsing/non_dataframe_string_fun.py.

camel_to_snake(value: str) -> str
"""

from ..unit_test_utils import assertion_helper

from transformations_library.parsing.non_dataframe_string_fun import camel_to_snake

STANDARD_MESSAGE = "Expected {0}, computed {1}"


def test_camel_to_snake_0():
    """
    Test the function: nothing in camel case.
    """
    output = camel_to_snake("already_snake_case")
    correct = "already_snake_case"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output))


def test_camel_to_snake_1():
    """
    Test the function: from camel to snake.
    """
    output = camel_to_snake("ThisNeedsToBeConverted")
    correct = "this_needs_to_be_converted"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output))


def test_camel_to_snake_2():
    """
    Test the function: from camel to snake, but _'s in place.
    """
    output = camel_to_snake("This_NeedsToBe_Converted")
    correct = "this__needs_to_be__converted"
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output))


def test_camel_to_snake_3():
    """
    Test the function: from camel to snake, but an acronym (all caps) not changed.
    """
    output = camel_to_snake("ThisIsAnABBREV")
    correct = "this_is_an_abbrev"  # should not put _'s between letters of ABBREV, does return as lower().
    assertion_helper(output, correct, STANDARD_MESSAGE.format(correct, output))

