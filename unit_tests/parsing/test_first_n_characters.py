"""
Test the first_n_character function in parsing/string_fun.py.

first_n_characters(
    df: DataFrame,
    columns: [dict, list, str],
    num_chars: int,
    default_name: str = None
) -> DataFrame:
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.string_fun import first_n_characters


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
        T.StructField("string", T.StringType(), True),
    ])

    DATA = [
        ["a", "hello world"],
        ["b", ""],
        ["c", "s"],
        ["d", "hi"],
        ["e", "fun"],
        ["f", None]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_first_n_characters_0(spark_session) -> None:
    """
    Test the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = first_n_characters(df, {"string": "new_column"}, 3)
    answer = {"a": "hel", "b": "", "c": "s", "d": "hi", "e": "fun", "f": None}
    answer_check(df, answer, "new_column")


def test_first_n_characters_1(spark_session) -> None:
    """
    Test the function: use default name.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = first_n_characters(df, "string", 3)
    answer = {"a": "hel", "b": "", "c": "s", "d": "hi", "e": "fun", "f": None}
    answer_check(df, answer, "string_first_3")


def test_first_n_characters_2(spark_session) -> None:
    """
    Test the function: override default name.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = first_n_characters(df, "string", 3, default_name="first_3_{0}")
    answer = {"a": "hel", "b": "", "c": "s", "d": "hi", "e": "fun", "f": None}
    answer_check(df, answer, "first_3_string")

