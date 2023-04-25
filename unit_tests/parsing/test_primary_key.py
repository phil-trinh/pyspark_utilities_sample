"""
Test the primary_key function in parsing/row_key.py.

primary_key(df: DataFrame, *columns: str, key_col: str = "primary_key", sep: str = "-") -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.row_key import primary_key


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("col_a", T.StringType(), True),
        T.StructField("col_b", T.StringType(), True),
        T.StructField("col_c", T.IntegerType(), True),
    ])

    DATA = [
        ["a", "hello", "world", 1],
        ["b", "hi", "there", 2],
        ["c", None, "bye", None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_primary_key_0(spark_session) -> None:
    """
    Test the function: use default values of inputs.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = primary_key(df, "col_a", "col_b")
    answer = {"a": "hello-world", "b": "hi-there", "c": "bye"}
    answer_check(df, answer, "key", key_col="id")


def test_primary_key_1(spark_session) -> None:
    """
    Test the function: ensure a non-string (integer) is converted; override default values of inputs.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = primary_key(df, "col_a", "col_b", "col_c", key_col="my_key", sep=":")
    answer = {"a": "hello:world:1", "b": "hi:there:2", "c": "bye"}
    answer_check(df, answer, "my_key", key_col="id")

