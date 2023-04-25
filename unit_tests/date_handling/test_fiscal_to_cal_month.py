"""
Test the fiscal_to_cal_month function in date_handling/to_calendar.py.

fiscal_to_cal_month(
    df: DataFrame,
    month_col: str,
    col_name: str = "calendar_month",
    str_type: bool = False,
    zero_pad: int = 1
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.date_handling import to_calendar


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
        T.StructField("month", T.StringType(), True),
    ])

    DATA = [
        ["a", "001"],
        ["b", "003"],
        ["c", "4"],
        ["d", None],
        ["e", "12"]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_fiscal_to_cal_month_0(spark_session) -> None:
    """
    Test the function: return as a string.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.fiscal_to_cal_month(df, "month", str_type=True, zero_pad=2)
    answer = {
        "a": "10",
        "b": "12",
        "c": "01",
        "d": None,
        "e": "09",
    }
    answer_check(df, answer, "calendar_month")


def test_fiscal_to_cal_month_1(spark_session) -> None:
    """
    Test the function: return as an int.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.fiscal_to_cal_month(df, "month")
    answer = {
        "a": 10,
        "b": 12,
        "c": 1,
        "d": None,
        "e": 9,
    }
    answer_check(df, answer, "calendar_month")


def test_fiscal_to_cal_month_2(spark_session) -> None:
    """
    Test the function: return as a string, zero pad at 1.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.fiscal_to_cal_month(df, "month", str_type=True, zero_pad=1)
    answer = {
        "a": "10",
        "b": "12",
        "c": "1",
        "d": None,
        "e": "9",
    }
    answer_check(df, answer, "calendar_month")


def test_fiscal_to_cal_month_3(spark_session) -> None:
    """
    Test the function: return as a string, zero pad at 3.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.fiscal_to_cal_month(df, "month", str_type=True, zero_pad=3)
    answer = {
        "a": "010",
        "b": "012",
        "c": "001",
        "d": None,
        "e": "009",
    }
    answer_check(df, answer, "calendar_month")

