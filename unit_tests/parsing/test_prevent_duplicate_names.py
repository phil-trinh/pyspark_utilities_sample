"""
Test the function prevent_duplicate_names in parsing/schema_ops.py.

prevent_duplicate_names(df1: DataFrame, df2: DataFrame, ignore: [str, list], append_value: str) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import assertion_helper

from transformations_library.parsing.schema_ops import prevent_duplicate_names


def create_first_df(spark_session) -> DataFrame:
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
        T.StructField("col_b", T.IntegerType(), True),
        T.StructField("col_c", T.IntegerType(), True),
    ])

    DATA = [
        ["a", None, None, None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def create_second_df(spark_session) -> DataFrame:
    """
    Create a second DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("col_a", T.StringType(), True),
        T.StructField("col_b", T.IntegerType(), True),
        T.StructField("col_b_df2", T.IntegerType(), True),
        T.StructField("column_c", T.IntegerType(), True),
    ])

    DATA = [
        ["a", None, None, None, None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_prevent_duplicate_names_0(spark_session) -> None:
    """
    Test the function: set columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df1 = create_first_df(spark_session)
    df2 = create_second_df(spark_session)
    df2 = prevent_duplicate_names(df1, df2, ["key", "col_a"], "_df2")
    compare_to = set(df2.columns)
    answer = {"key", "col_a", "col_b_df2", "col_b_df2_1", "column_c"}
    assertion_helper(compare_to, answer, f"Computed {compare_to} but expected {answer}.")


def test_prevent_duplicate_names_1(spark_session) -> None:
    """
    Test the function: columns dropped instead of renamed.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df1 = create_first_df(spark_session)
    df2 = create_second_df(spark_session)
    df2 = prevent_duplicate_names(df1, df2, ["key", "col_a"])
    compare_to = set(df2.columns)
    answer = {"key", "col_a", "col_b_df2", "column_c"}
    assertion_helper(compare_to, answer, f"Computed {compare_to} but expected {answer}.")

