"""
Test the fiscal_month_name function in date_handling/to_fiscal.py.

fiscal_month_name(
    df: DataFrame,
    month_column: [dict, list, str],
    full_name: bool = True,
    upper_case: bool = True,ca
    default_name: str = "{0}_fiscal_month_name"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.date_handling import to_fiscal


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("month_num", T.IntegerType(), True),
        T.StructField("month_str", T.StringType(), True),
    ])

    DATA = [
        ["a", 1, "1"],
        ["b", 2, "2"],
        ["c", 3, "3"],
        ["d", 4, "4"],
        ["e", 5, "5"],
        ["f", 6, "6"],
        ["g", 7, "7"],
        ["h", 8, "8"],
        ["i", 9, "9"],
        ["j", 10, "10"],
        ["k", 11, "11"],
        ["l", 12, "12"],
        ["m", 13, "13"],
        ["n", None, None],
        ["o", 0, "0"],
        ["p", -1, "-1"],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_fiscal_month_name_0(spark_session) -> None:
    """
    Test the function: return as a short version.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    # This uses full_name = False, will cut down full month name to 3 characters
    df = create_dataframe(spark_session)
    columns = {"month_num": "name_from_num", "month_str": "name_from_str"}
    df = to_fiscal.fiscal_month_name(df, columns, full_name=False)

    answer = {
        "a": "OCT",
        "b": "NOV",
        "c": "DEC",
        "d": "JAN",
        "e": "FEB",
        "f": "MAR",
        "g": "APR",
        "h": "MAY",
        "i": "JUN",
        "j": "JUL",
        "k": "AUG",
        "l": "SEP",
        "m": None,
        "n": None,
        "o": None,
        "p": None,
    }

    answer_check(df, answer, "name_from_num", "number_test")
    answer_check(df, answer, "name_from_str", "string_test")


def test_fiscal_month_name_1(spark_session) -> None:
    """
    Test the function: return as a full name, all caps version.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    columns = {"month_num": "name_from_num", "month_str": "name_from_str"}
    df = to_fiscal.fiscal_month_name(df, columns, full_name=True)

    answer = {
        "a": "OCTOBER",
        "b": "NOVEMBER",
        "c": "DECEMBER",
        "d": "JANUARY",
        "e": "FEBRUARY",
        "f": "MARCH",
        "g": "APRIL",
        "h": "MAY",
        "i": "JUNE",
        "j": "JULY",
        "k": "AUGUST",
        "l": "SEPTEMBER",
        "m": None,
        "n": None,
        "o": None,
        "p": None,
    }

    answer_check(df, answer, "name_from_num", "number_test")
    answer_check(df, answer, "name_from_str", "string_test")


def test_fiscal_month_name_2(spark_session) -> None:
    """
    Test the function: return as a full name, normal case version.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    columns = {"month_num": "name_from_num", "month_str": "name_from_str"}
    df = to_fiscal.fiscal_month_name(df, columns, full_name=True, upper_case=False)

    answer = {
        "a": "October",
        "b": "November",
        "c": "December",
        "d": "January",
        "e": "February",
        "f": "March",
        "g": "April",
        "h": "May",
        "i": "June",
        "j": "July",
        "k": "August",
        "l": "September",
        "m": None,
        "n": None,
        "o": None,
        "p": None,
    }

    answer_check(df, answer, "name_from_num", "number_test")
    answer_check(df, answer, "name_from_str", "string_test")

