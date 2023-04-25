"""
Test the date_to_fiscal_month function in date_handling/to_fiscal.py.

date_to_fiscal_month(
    df: DataFrame,
    date_col: str,
    col_name: str = "fiscal_month",
    str_type: bool = False,
    left_padded: bool = False
) -> DataFrame
"""

from pyspark.sql import types as T, functions as F, DataFrame
from datetime import datetime

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
        T.StructField("date", T.DateType(), True)
    ])

    DATA = [
        ["a", datetime(2022, 9, 28)],
        ["b", datetime(2022, 10, 1)],
        ["c", None],
        ["d", datetime(2022, 11, 1)],
        ["e", datetime(2022, 12, 1)],
        ["f", datetime(2023, 1, 1)],
        ["g", datetime(2023, 2, 1)],
        ["h", datetime(2023, 3, 1)],
        ["i", datetime(2023, 4, 1)],
        ["j", datetime(2023, 5, 1)],
        ["k", datetime(2023, 6, 1)],
        ["l", datetime(2023, 7, 1)],
        ["m", datetime(2023, 8, 1)]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_date_to_fiscal_month_0(spark_session) -> None:
    """
    Test the function: return as an integer.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.date_to_fiscal_month(df, "date")
    answer = {
        "a": 12,
        "b": 1,
        "c": None,
        "d": 2,
        "e": 3,
        "f": 4,
        "g": 5,
        "h": 6,
        "i": 7,
        "j": 8,
        "k": 9,
        "l": 10,
        "m": 11
    }
    answer_check(df, answer, "fiscal_month")


def test_date_to_fiscal_month_1(spark_session) -> None:
    """
    Test the function: return as a string with leading "0" if only 1 character.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.date_to_fiscal_month(df, "date", str_type=True, left_padded=True)
    answer = {
        "a": "12",
        "b": "01",
        "c": None,
        "d": "02",
        "e": "03",
        "f": "04",
        "g": "05",
        "h": "06",
        "i": "07",
        "j": "08",
        "k": "09",
        "l": "10",
        "m": "11"
    }
    answer_check(df, answer, "fiscal_month")

def test_date_to_fiscal_month_2(spark_session) -> None:
    """
    Test the function: change input to TimestampType, return fiscal month as an integer.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.date_to_fiscal_month(df, "date")
    answer = {
        "a": 12,
        "b": 1,
        "c": None,
        "d": 2,
        "e": 3,
        "f": 4,
        "g": 5,
        "h": 6,
        "i": 7,
        "j": 8,
        "k": 9,
        "l": 10,
        "m": 11
    }
    df = df.withColumn("date", F.col("date").cast(T.TimestampType()))
    answer_check(df, answer, "fiscal_month")


def test_date_to_fiscal_month_3(spark_session) -> None:
    """
    Test the function: return as a string with no leading characters.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.date_to_fiscal_month(df, "date", str_type=True, left_padded=False)
    answer = {
        "a": "12",
        "b": "1",
        "c": None,
        "d": "2",
        "e": "3",
        "f": "4",
        "g": "5",
        "h": "6",
        "i": "7",
        "j": "8",
        "k": "9",
        "l": "10",
        "m": "11"
    }
    answer_check(df, answer, "fiscal_month")

