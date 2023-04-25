"""
Test the replace_characters function in parsing/string_fun.py.

replace_characters(
    df: DataFrame,
    columns: [dict, list, str],
    default_name: str = "{0}",
    remove_chars: [dict, str] = " _",
    default_replacement: str = "",
    universal: bool = False
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.string_fun import replace_characters


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
        ["a", "  __  hello"],
        ["b", "hi _ _ _ _"],
        ["c", None],
        ["d", "____ _ both_sides_and middle  ___ _"],
        ["e", "$$$$&other_characters__)))))"],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_replace_characters_0(spark_session) -> None:
    """
    Test the function: use default values of inputs.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, {"col_a": "output"})
    answer = {"a": "hello", "b": "hi", "c": None, "d": "both_sides_and middle", "e": "$$$$&other_characters__)))))"}
    answer_check(df, answer, "output")


def test_replace_characters_1(spark_session) -> None:
    """
    Test the function: use different char_set and overwrite original column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, "col_a", remove_chars="$_)")
    answer = {
        "a": "  __  hello",
        "b": "hi _ _ _ ",
        "c": None,
        "d": " _ both_sides_and middle  ___ ",
        "e": "&other_characters"
    }
    answer_check(df, answer, "col_a")


def test_replace_characters_2(spark_session) -> None:
    """
    Test the function: try replacing default values with ~~ and overwrite original column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, "col_a", default_replacement="~~")
    answer = {
        "a": "~~~~~~~~~~~~hello",
        "b": "hi~~~~~~~~~~~~~~~~",
        "c": None,
        "d": "~~~~~~~~~~~~~~both_sides_and middle~~~~~~~~~~~~~~",
        "e": "$$$$&other_characters__)))))"
    }
    answer_check(df, answer, "col_a")


def test_replace_characters_3(spark_session) -> None:
    """
    Test the function: try replacing space with *, but a _ with a %, ensure default_replacement isn't used, and
    overwrite original column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, "col_a", remove_chars={" ": "*", "_": "%"}, default_replacement="~~")
    answer = {
        "a": "**%%**hello",
        "b": "hi*%*%*%*%",
        "c": None,
        "d": "%%%%*%*both_sides_and middle**%%%*%",
        "e": "$$$$&other_characters__)))))"
    }
    answer_check(df, answer, "col_a")


def test_replace_characters_4(spark_session) -> None:
    """
    Test the function: try replacing space and _ universally with # and write to a new column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, {"col_a": "out_a"}, universal=True, default_replacement="#")
    answer = {
        "a": "######hello",
        "b": "hi########",
        "c": None,
        "d": "#######both#sides#and#middle#######",
        "e": "$$$$&other#characters##)))))"
    }
    answer_check(df, answer, "out_a")


def test_replace_characters_5(spark_session) -> None:
    """
    Test the function: use a list of characters.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, {"col_a": "output"}, remove_chars=[" ", "_"])
    answer = {"a": "hello", "b": "hi", "c": None, "d": "both_sides_and middle", "e": "$$$$&other_characters__)))))"}
    answer_check(df, answer, "output")


def test_replace_characters_6(spark_session) -> None:
    """
    Test the function: call function multiple times to test how it works in combination.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, {"col_a": "values"}, {"_": " "}, universal=True)
    df = replace_characters(df, "values", {" ": ""})
    answer = {"a": "hello", "b": "hi", "c": None, "d": "both sides and middle", "e": "$$$$&other characters  )))))"}
    answer_check(df, answer, "values")


def test_replace_characters_7(spark_session) -> None:
    """
    Test the function: call function multiple times to test how it works in combination.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = replace_characters(df, "col_a", {"_": " "}, universal=True, collapse=True)
    answer = {"a": " hello", "b": "hi ", "c": None, "d": " both sides and middle ", "e": "$$$$&other characters )))))"}
    answer_check(df, answer, "col_a")

