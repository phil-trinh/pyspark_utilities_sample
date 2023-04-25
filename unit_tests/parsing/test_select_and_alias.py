"""
Test the select_and_alias function in parsing/schema_ops.py.

select_and_alias(df: DataFrame, col_mapping: [dict, list]) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import assertion_helper

from transformations_library.parsing.schema_ops import select_and_alias


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session - Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), True),
        T.StructField("col1", T.DateType(), True),
        T.StructField("col2", T.IntegerType(), True),
        T.StructField("col3", T.BooleanType(), True),
    ])

    DATA = [
        [None, None, None, None]
     ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_select_and_alias_0(spark_session) -> None:
    """
    Test the function: list style, ensure columns are not renamed.

    Input
        spark_session - Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = select_and_alias(df, ["key", "col2", "col3"])  # Drop col1.
    compare_to = set(df.columns)
    answer = {"col2", "col3", "key"}
    assertion_helper(compare_to, answer, f"Computed {compare_to} but expected {answer}.")


def test_select_and_alias_1(spark_session) -> None:
    """
    Test the function: dictionary style, ensure columns get new names.

    Input
        spark_session - Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = select_and_alias(df, {"key": "primary_key", "col2": "column2", "col3": "col3"})  # Drop col1.
    compare_to = set(df.columns)
    answer = {"col3", "column2", "primary_key"}
    assertion_helper(compare_to, answer, f"Computed {compare_to} but expected {answer}.")


def test_select_and_alias_2(spark_session) -> None:
    """
    Test the function: dictionary style, ensure columns get new names, don't drop anything.

    Input
        spark_session - Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = select_and_alias(df, {"key": "primary_key", "col2": "column2", "col3": "col3", "col1": "col1"})
    compare_to = set(df.columns)
    answer = {"col1", "col3", "column2", "primary_key"}
    assertion_helper(compare_to, answer, f"Computed {compare_to} but expected {answer}.")

