"""
Test the date_to_fiscal_year function in date_handling/to_fiscal.py.

date_to_fiscal_year(
    df: DataFrame,
    date_col: str,
    col_name: str = "fiscal_year",
    str_type: bool = False,
    fy_shorthand: bool = False
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
        ["c", None]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_date_to_fiscal_year_0(spark_session) -> None:
    """
    Test the function: return as an integer.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.date_to_fiscal_year(df, "date")
    answer = {
        "a": 2022,
        "b": 2023,
        "c": None
    }
    answer_check(df, answer, "fiscal_year")


def test_date_to_fiscal_year_1(spark_session) -> None:
    """
    Test the function: return as a string in shorthand format (with "FY" prefix followed by last 2 characters).

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.date_to_fiscal_year(df, "date", fy_shorthand=True)
    answer = {
        "a": "FY22",
        "b": "FY23",
        "c": None
    }
    answer_check(df, answer, "fiscal_year")
    

def test_date_to_fiscal_year_2(spark_session) -> None:
    """
    Test the function: change input to TimestampType, return fiscal year as an integer.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.date_to_fiscal_year(df, "date")
    answer = {
        "a": 2022,
        "b": 2023,
        "c": None
    }
    df = df.withColumn("date", F.col("date").cast(T.TimestampType()))
    answer_check(df, answer, "fiscal_year")

