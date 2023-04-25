"""
Test the first_n_digits function in parsing/digit_fun.py.

first_n_digits(
    df: DataFrame,
    columns: [dict, list, str],
    num_digits: int = 1,
    default_name: str = "{0}_leading"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.digit_fun import first_n_digits


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
        T.StructField("number", T.IntegerType(), True),
    ])

    DATA = [
        ["a", 123456],
        ["b", 123],
        ["c", 1],
        ["d", -123],
        ["e", None]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_first_n_digits_0(spark_session) -> None:
    """
    Test the function: default of 1 digit.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = first_n_digits(df, {"number": "new_column"})
    answer = {"a": 1, "b": 1, "c": 1, "d": 1, "e": None}
    answer_check(df, answer, "new_column")


def test_first_n_digits_1(spark_session) -> None:
    """
    Test the function: use default name and 4 digits.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = first_n_digits(df, "number", 4)
    answer = {"a": 1234, "b": 123, "c": 1, "d": 123, "e": None}
    answer_check(df, answer, "number_leading")

