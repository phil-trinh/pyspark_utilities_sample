"""
Test the percentiles function in modeling/percentiles.py.

percentiles(
    df: DataFrame,
    percentiles: list,
    group_col: str,
    agg_cols: [list, str],
    new_col_name: str = "percentile"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.row_key import primary_key
from transformations_library.modeling.percentiles import percentiles


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
        T.StructField("second_group", T.StringType(), False),
        T.StructField("data", T.IntegerType(), True),
        T.StructField("data_two", T.IntegerType(), True),
    ])

    DATA = [
        ["a", "hello", 1, 100],
        ["a", "world", 2, 200],
        ["a", "hello", 3, 300],
        ["a", "world", 4, 400],
        ["b", "hello", 10, None],
        ["b", "world", 20, 50],
        ["b", "hello", 30, 60],
        ["b", "world", None, 70]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_percentiles_0(spark_session) -> None:
    """
    Test the function: one percentile on one column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percentiles(df, [50], "key", "data")

    answer = {"a": 2.5, "b": 20}
    answer_check(df, answer, "percentile_50_data")


def test_percentiles_1(spark_session) -> None:
    """
    Test the function: using two percentile inputs on one column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percentiles(df, [25, 50], "key", "data")

    answer_25 = {"a": 1.75, "b": 15}
    answer_check(df, answer_25, "percentile_25_data", "test for 25%")

    answer_50 = {"a": 2.5, "b": 20}
    answer_check(df, answer_50, "percentile_50_data", "test for 50%")


def test_percentiles_2(spark_session) -> None:
    """
    Test the function: using two percentile inputs on two columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percentiles(df, [25, 50], "key", ["data", "data_two"])

    answer_25_data = {"a": 1.75, "b": 15}
    answer_check(df, answer_25_data, "percentile_25_data", "test for 25% on data")

    answer_50_data = {"a": 2.5, "b": 20}
    answer_check(df, answer_50_data, "percentile_50_data", "test for 50% on data")

    answer_25_datatwo = {"a": 175, "b": 55}
    answer_check(df, answer_25_datatwo, "percentile_25_data_two", "test for 25% on data two")

    answer_50_datatwo = {"a": 250, "b": 60}
    answer_check(df, answer_50_datatwo, "percentile_50_data_two", "test for 50% on data two")


def test_percentiles_3(spark_session) -> None:
    """
    Test the function: using two group-by columns for two percentile inputs on two columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percentiles(df, [25, 50], ["key", "second_group"], ["data", "data_two"])
    # Concatenate the two group-by columns to make one key for the answer_check function to be able to compare answers.
    df = primary_key(df, "key", "second_group", key_col="id")

    answer_25_data = {"a-hello": 1.5, "a-world": 2.5, "b-hello": 15, "b-world": 20}
    answer_check(df, answer_25_data, "percentile_25_data", "test for 25% on data", key_col="id")

    answer_50_data = {"a-hello": 2, "a-world": 3, "b-hello": 20, "b-world": 20}
    answer_check(df, answer_50_data, "percentile_50_data", "test for 50% on data", key_col="id")

    answer_25_datatwo = {"a-hello": 150, "a-world": 250, "b-hello": 60, "b-world": 55}
    answer_check(df, answer_25_datatwo, "percentile_25_data_two", "test for 25% on data two", key_col="id")

    answer_50_datatwo = {"a-hello": 200, "a-world": 300, "b-hello": 60, "b-world": 60}
    answer_check(df, answer_50_datatwo, "percentile_50_data_two", "test for 50% on data two", key_col="id")

