"""
Test the cumul_sum function in modeling/cumulative.py.

cumul_sum(
    df: DataFrame,
    columns: [dict, list, str],
    cols_order: [list, str],
    cols_grp: [list, str] = None,
    default_name: str = "{0}_cumul_sum"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.cumulative import cumul_sum


def cumul_sum_df(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("sort", T.StringType(), True),
        T.StructField("group", T.StringType(), True),
        T.StructField("to_add", T.IntegerType(), True),
        T.StructField("to_add_also", T.IntegerType(), True),
    ])

    DATA = [
        ["a", "sort1", "group1", 1, 30],
        ["b", "sort2", "group1", 2, 40],
        ["c", "sort2", "group2", 1, 100],
        ["d", "sort1", "group2", 4, 200],
        ["e", "sort3", "group1", 8, 80],
        ["f", "sort3", "group2", None, None]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_cumul_sum_0(spark_session) -> None:
    """
    Test the function: dictionary style to name the output column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = cumul_sum_df(spark_session)
    cols_target = {"to_add": "cumulative_column"}
    df = cumul_sum(df, cols_target, "sort", "group")
    answer = {"a": 1, "b": 3, "c": 5, "d": 4, "e": 11, "f": 5}
    answer_check(df, answer, "cumulative_column", "test_0")


def test_cumul_sum_1(spark_session):
    """
    Test the function: string style to name the output column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = cumul_sum_df(spark_session)
    df = cumul_sum(df, "to_add", "sort", "group")
    answer = {"a": 1, "b": 3, "c": 5, "d": 4, "e": 11, "f": 5}
    answer_check(df, answer, "to_add_cumul_sum", "test_1")


def test_cumul_sum_2(spark_session):
    """
    Test the function: ensure it creates two separate columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = cumul_sum_df(spark_session)
    df = cumul_sum(df, ["to_add", "to_add_also"], "sort", "group")
    answer = {"a": 1, "b": 3, "c": 5, "d": 4, "e": 11, "f": 5}
    answer_check(df, answer, "to_add_cumul_sum", "test_2_to_add")
    answer = {"a": 30, "b": 70, "c": 300, "d": 200, "e": 150, "f": 300}
    answer_check(df, answer, "to_add_also_cumul_sum", "test_2_to_add_also")


def test_cumul_sum_3(spark_session):
    """
    Test the function : try with no group-by column and two order-by columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = cumul_sum_df(spark_session)
    cols_target = {"to_add": "cumulative_column"}
    df = cumul_sum(df, cols_target, ["sort", "key"])
    answer = {"a": 1, "b": 7, "c": 8, "d": 5, "e": 16, "f": 16}
    answer_check(df, answer, "cumulative_column", "test_3")


def test_cumul_sum_4(spark_session):
    """
    Test the function: order-by input in a list.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = cumul_sum_df(spark_session)
    df = cumul_sum(df, "to_add", "sort", ["group"])
    answer = {"a": 1, "b": 3, "c": 5, "d": 4, "e": 11, "f": 5}
    answer_check(df, answer, "to_add_cumul_sum", "test_4")


def test_cumul_sum_5(spark_session):
    """
    Test the function: override the default_name input.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = cumul_sum_df(spark_session)
    df = cumul_sum(df, "to_add", "sort", ["group"], default_name="output_{0}")
    answer = {"a": 1, "b": 3, "c": 5, "d": 4, "e": 11, "f": 5}
    answer_check(df, answer, "output_to_add", "test_5")
