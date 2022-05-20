"""
Test the forward_fill function in modeling/forward_fill.py.

forward_fill(
    df: DataFrame,
    columns: [dict, list, str],
    ordering: str,
    partitioning: [list, str] = None,
    default_name: str = "{0}"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.forward_fill import forward_fill


def forward_fill_df(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("data", T.IntegerType(), True),
        T.StructField("order-by", T.StringType(), True),
        T.StructField("group-by", T.StringType(), True),
    ])

    DATA = [
        ["a", None, "a", "1"],
        ["b", 2, "b", "2"],
        ["c", None, "c", "1"],
        ["d", 4, "d", "1"],
        ["e", 8, "e", "2"],
        ["f", None, "f", "2"],
        ["g", None, "g", "1"]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_forward_fill_0(spark_session) -> None:
    """
    Test the function: no group-by column, overwrite existing data.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = forward_fill_df(spark_session)
    df = forward_fill(df, "data", "order-by")
    answer = {"a": None, "b": 2, "c": 2, "d": 4, "e": 8, "f": 8, "g": 8}
    answer_check(df, answer, "data", "test_0")


def test_forward_fill_1(spark_session) -> None:
    """
    Test the function: no group-by column, provide new column instead of overwriting existing.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = forward_fill_df(spark_session)
    df = forward_fill(df, {"data": "new_data"}, "order-by")
    answer = {"a": None, "b": 2, "c": 2, "d": 4, "e": 8, "f": 8, "g": 8}
    answer_check(df, answer, "new_data", "test_1 of output column")
    answer = {"a": None, "b": 2, "c": None, "d": 4, "e": 8, "f": None, "g": None}
    answer_check(df, answer, "data", "test_1 of original column")


def test_forward_fill_2(spark_session) -> None:
    """
    Test the function: group-by column, list style, override default name.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = forward_fill_df(spark_session)
    df = forward_fill(df, "data", "order-by", partitioning="group-by", default_name="{0}_out")
    answer = {"a": None, "b": 2, "c": None, "d": 4, "e": 8, "f": 8, "g": 4}
    answer_check(df, answer, "data_out", "test_2")
