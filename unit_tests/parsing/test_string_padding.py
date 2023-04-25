"""
Test the function string_padding from parsing/string_fun.py.

string_padding(df: DataFrame, column: str, width: int, character: str = "0") -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.string_fun import string_padding


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
        T.StructField("col_a", T.StringType(), True),
    ])

    DATA = [
        ["a", "1"],
        ["b", "22"],
        ["c", None],
        ["d", "333"],
        ["e", "4444"],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_string_padding_0(spark_session):
    """
    Test the function: add a 0 to all entries.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = string_padding(df, "col_a", 5, "0")
    answer = {"a": "00001", "b": "00022", "c": None, "d": "00333", "e": "04444"}
    answer_check(df, answer, "col_a")


def test_string_padding_1(spark_session):
    """
    Test the function: add a X to some entries, ensure the others are not shortened.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = string_padding(df, "col_a", 3, "X")
    answer = {"a": "XX1", "b": "X22", "c": None, "d": "333", "e": "4444"}
    answer_check(df, answer, "col_a")

