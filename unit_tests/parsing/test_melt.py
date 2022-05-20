"""
Test the melt function in parsing/melting.py.

melt(
    df: DataFrame,
    id_vars: Iterable[str] = None,
    value_vars: Iterable[str] = None,
    var_name: str = "categories",
    value_name: str = "values"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.melting import melt
from transformations_library.parsing.row_key import primary_key


def melt_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the melt function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("col_a", T.StringType(), True),
        T.StructField("col_b", T.IntegerType(), True),
        T.StructField("col_c", T.IntegerType(), True),
    ])

    DATA = [
        ["a", "hello", 1, 2],
        ["b", "world", 3, None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_melt_0(spark_session) -> None:
    """
    Test the function: set columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = melt_dataframe(spark_session)
    df = melt(df, id_vars=["key"], value_vars=["col_a", "col_b", "col_c"])
    df = primary_key(df, "key", "categories")
    answer = {"a-col_a": "hello", "a-col_b": "1", "a-col_c": "2", "b-col_a": "world", "b-col_b": "3", "b-col_c": None}
    answer_check(df, answer, "values", "test_0", key_col="primary_key")


def test_melt_1(spark_session) -> None:
    """
    Test the function: let one input be None to ensure it picks up the rest of the columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = melt_dataframe(spark_session)
    df = melt(df, id_vars=["key"])
    df = primary_key(df, "key", "categories")
    answer = {"a-col_a": "hello", "a-col_b": "1", "a-col_c": "2", "b-col_a": "world", "b-col_b": "3", "b-col_c": None}
    answer_check(df, answer, "values", "test_1", key_col="primary_key")


def test_melt_2(spark_session) -> None:
    """
    Test the function: let one input be None to ensure it picks up the rest of the columns - opposite of test_1.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = melt_dataframe(spark_session)
    df = melt(df, value_vars=["col_a", "col_b", "col_c"])
    df = primary_key(df, "key", "categories")
    answer = {"a-col_a": "hello", "a-col_b": "1", "a-col_c": "2", "b-col_a": "world", "b-col_b": "3", "b-col_c": None}
    answer_check(df, answer, "values", "test_2", key_col="primary_key")
