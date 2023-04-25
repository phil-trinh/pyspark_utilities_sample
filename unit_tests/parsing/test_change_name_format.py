"""
Test the change_name_function in parsing/name_format.py.

change_name_format(
    df: DataFrame,
    columns: [str, list, dict],
    sep: str = "\\.",
    default_name: str = "{0}"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.name_format import change_name_format


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
        T.StructField("name", T.StringType(), True),
        T.StructField("name_other", T.StringType(), True)
    ])

    DATA = [
        ["a", "Roberts.Bill", "Roberts-Bill"],
        ["b", "Brown.Cindy", "Brown-Cindy"],
        ["c", "Annef.Lisa", "Annef-Lisa"],
        ["d", "", ""],
        ["e", None, None],
     ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_change_name_format_0(spark_session) -> None:
    """
    Test the function: See if last.first is converted to last, first, with period delimiter

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = change_name_format(df, "name")

    answer = {
        "a": "Roberts, Bill",
        "b": "Brown, Cindy",
        "c": "Annef, Lisa",
        "d": "",
        "e": "",
    }

    answer_check(df, answer, "name")


def test_change_name_format_1(spark_session) -> None:
    """
    Test the function: See if last-first is converted to last, first, with dash delimiter

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = change_name_format(df, "name_other", sep="-")

    answer = {
        "a": "Roberts, Bill",
        "b": "Brown, Cindy",
        "c": "Annef, Lisa",
        "d": "",
        "e": "",
    }

    answer_check(df, answer, "name_other")

