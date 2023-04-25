"""
Test the underscore_to_space function in parsing/string_fun.py.

underscore_to_space(df: DataFrame, columns: [dict, list, str], default_name: str = "{0}") -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.string_fun import underscore_to_space


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
        ["a", "__hello_world__"],
        ["b", "hi_________there__world"],
        ["c", None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_underscore_to_space_0(spark_session) -> None:
    """
    Test the function: use default values of inputs.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = underscore_to_space(df, "col_a")
    answer = {"a": "hello world", "b": "hi there world", "c": None}
    answer_check(df, answer, "col_a")


def test_underscore_to_space_1(spark_session) -> None:
    """
    Test the function: use default values of inputs; test that going to a new column leaves original unchanged.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = underscore_to_space(df, {"col_a": "output"})
    answer = {"a": "__hello_world__", "b": "hi_________there__world", "c": None}
    answer_check(df, answer, "col_a", "test_1_original")
    answer = {"a": "hello world", "b": "hi there world", "c": None}
    answer_check(df, answer, "output", "test_1_output")

