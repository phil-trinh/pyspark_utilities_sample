"""
Test the row_number_key function in parsing/row_key.py.

row_number_key(df: DataFrame, ordering_column: str, num_col: str = "row_number", as_str: bool = True) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.row_key import row_number_key


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
        T.StructField("ordering2", T.StringType(), True),
    ])

    DATA = [
        ["a", "bb"],
        ["b", "aa"],
        ["c", None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_row_number_key_0(spark_session) -> None:
    """
    Test the function: use default values of inputs.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = row_number_key(df, "key")
    answer = {"a": "1", "b": "2", "c": "3"}
    answer_check(df, answer, "row_number")


def test_row_number_key_1(spark_session) -> None:
    """
    Test the function: use default values of inputs, column with a null.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = row_number_key(df, "ordering2")
    answer = {"a": "3", "b": "2", "c": "1"}
    answer_check(df, answer, "row_number")


def test_row_number_key_2(spark_session) -> None:
    """
    Test the function: override default values of inputs.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = row_number_key(df, "key", num_col="new_column", as_str=False)
    answer = {"a": 1, "b": 2, "c": 3}
    answer_check(df, answer, "new_column")

