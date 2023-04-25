"""
Test the convert_unix_time function in date_handling/to_calendar.py.

convert_unix_time(
    df: DataFrame,
    unix_timestamp_cols: [dict, list, str],
    date_only: bool = False,
    default_name: str = "{0}"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame
import datetime

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
        T.StructField("unix_timestamp", T.StringType(), True),
    ])

    DATA = [
        ["a", -4156000],
        ["b", -7000004156000],
        ["c", 7000],
        ["d", None],
        ["e", 5000145672000],
        ["f", 3012470125000],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_convert_unix_time_0(spark_session) -> None:
    """
    Test the function: return as a time.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.convert_unix_time(df, {"unix_timestamp": "timestamp"})

    answer = {
        "a": datetime.datetime(1969, 12, 31, 22, 50, 44),
        "b": datetime.datetime(1748, 3, 6, 10, 24, 4),
        "c": datetime.datetime(1970, 1, 1, 0, 0, 7),
        "d": None,
        "e": datetime.datetime(2128, 6, 13, 1, 21, 12),
        "f": datetime.datetime(2065, 6, 17, 13, 15, 25),
    }

    answer_check(df, answer, "timestamp")


def test_convert_unix_time_1(spark_session) -> None:
    """
    Test the function: return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.convert_unix_time(df, {"unix_timestamp": "timestamp"}, date_only=True)

    answer = {
        "a": datetime.date(1969, 12, 31),
        "b": datetime.date(1748, 3, 6),
        "c": datetime.date(1970, 1, 1),
        "d": None,
        "e": datetime.date(2128, 6, 13),
        "f": datetime.date(2065, 6, 17),
    }

    answer_check(df, answer, "timestamp")


def test_convert_unix_time_2(spark_session) -> None:
    """
    Test the function: return as a date using the default output column name.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.convert_unix_time(df, ["unix_timestamp"], date_only=True)

    answer = {
        "a": datetime.date(1969, 12, 31),
        "b": datetime.date(1748, 3, 6),
        "c": datetime.date(1970, 1, 1),
        "d": None,
        "e": datetime.date(2128, 6, 13),
        "f": datetime.date(2065, 6, 17),
    }

    answer_check(df, answer, "unix_timestamp")

