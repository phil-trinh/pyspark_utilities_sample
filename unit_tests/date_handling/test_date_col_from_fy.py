"""
Test the date_col_from_fy function in date_handling/to_calendar.py.

date_col_from_fy(df: DataFrame, fy_col: str, date_col: str, quarterly: bool = False) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame
from datetime import datetime

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
        T.StructField("data", T.StringType(), True),
        T.StructField("data_no_quarter", T.StringType(), True),
    ])

    DATA = [
        ["a", "FY21Q4", "FY21"],
        ["b", "FY20Q1", "FY20"],
        ["c", "FY18Q2", "FY18"],
        ["d", None, None],
        ["e", "FY19Q3", "FY19"]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_date_col_from_fy_0(spark_session) -> None:
    """
    Test the function: quarterly designation.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.date_col_from_fy(df, "data", "output", True)
    answer = {
        "a": datetime(2021, 9, 30).date(),
        "b": datetime(2019, 12, 31).date(),
        "c": datetime(2018, 3, 31).date(),
        "d": None,
        "e": datetime(2019, 6, 30).date(),
    }
    answer_check(df, answer, "output")


def test_date_col_from_fy_1(spark_session) -> None:
    """
    Test the function date_col_from_fy: no quarterly designation

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.date_col_from_fy(df, "data_no_quarter", "output")
    answer = {
        "a": datetime(2020, 12, 31).date(),
        "b": datetime(2019, 12, 31).date(),
        "c": datetime(2017, 12, 31).date(),
        "d": None,
        "e": datetime(2018, 12, 31).date(),
    }
    answer_check(df, answer, "output")

