"""
Test the drop_null_columns function in modeling/null_fun.py.

drop_null_columns(df: DataFrame, threshold: float = 1.0, subset: list = None) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.null_fun import drop_null_columns


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
        T.StructField("zero", T.IntegerType(), True),
        T.StructField("sixty", T.IntegerType(), True),
        T.StructField("full", T.IntegerType(), True),
    ])

    DATA = [
        ["a", 1, 11, None],
        ["b", 2, 12, None],
        ["c", 3, None, None],
        ["d", 4, None, None],
        ["e", 5, None, None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_drop_null_columns_0(spark_session) -> None:
    """
    Test the function: set threshold to 100%.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = drop_null_columns(df, 1.0)
    answer = {"key", "zero", "sixty"}
    assert set(df.columns) == answer, f"Expected {answer}, but computed {df.columns}"

    unchanged_value = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
    answer_check(df, unchanged_value, "zero")


def test_drop_null_columns_1(spark_session) -> None:
    """
    Test the function: set threshold to 60%.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = drop_null_columns(df, 0.6)
    answer = {"key", "zero"}
    assert set(df.columns) == answer, f"Expected {answer}, but computed {df.columns}"

    unchanged_value = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
    answer_check(df, unchanged_value, "zero")


def test_drop_null_columns_2(spark_session) -> None:
    """
    Test the function: set threshold to 60%, look at a subset of columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = drop_null_columns(df, subset=["key", "zero", "sixty"], threshold=0.6)
    answer = {"key", "zero", "full"}
    assert set(df.columns) == answer, f"Expected {answer}, but computed {df.columns}"

    unchanged_value = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
    answer_check(df, unchanged_value, "zero")


def test_drop_null_columns_3(spark_session) -> None:
    """
    Test the function: set threshold to 60%, look at a subset of columns; no columns should drop.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = drop_null_columns(df, subset=["key"], threshold=0.6)
    answer = {"key", "zero", "sixty", "full"}
    assert set(df.columns) == answer, f"Expected {answer}, but computed {df.columns}"

    unchanged_value = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
    answer_check(df, unchanged_value, "zero")

