"""
Test the multi_column_formatter function in parsing/string_fun.py.

multi_column_formatter(df: DataFrame, new_column_name: str, lambda_fun: Callable, *columns: list) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.string_fun import multi_column_formatter


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the multi_column_formatter function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("string1", T.StringType(), True),
        T.StructField("string2", T.StringType(), True),
        T.StructField("number", T.IntegerType(), True),
    ])

    DATA = [
        ["a", "hello world", "goodbye", 10],
        ["b", "", None, 100],
        ["c", "short", "string", None],
        ["d", None, None, 20]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_multi_column_0(spark_session) -> None:
    """
    Test the function: use two columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = multi_column_formatter(df, "new_column", lambda x, y: f"{x}:{y}", "string1", "string2")
    answer = {"a": "hello world:goodbye", "b": ":None", "c": "short:string", "d": "None:None"}
    answer_check(df, answer, "new_column")


def test_multi_column_1(spark_session) -> None:
    """
    Test the function: use two columns, replace null with na.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    def format_fun(x, y):
        """
        Formatting function for a specific use-case. Will be converted to the UDF inside the main function
        """
        if x is None and y is None:
            return "N/A"
        if x is None:
            x = "na"
        if y is None:
            y = "na"
        return f"{x}-{y}"
    df = create_dataframe(spark_session)
    df = multi_column_formatter(df, "new_column", format_fun, "string1", "string2")
    answer = {"a": "hello world-goodbye", "b": "-na", "c": "short-string", "d": "N/A"}
    answer_check(df, answer, "new_column")


def test_multi_column_2(spark_session) -> None:
    """
    Test the function: use three columns, replace null with na, format a number with zero-padding.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    def format_fun(x, y, z):
        """
        Formatting function for a specific use-case. Will be converted to the UDF inside the main function
        """
        if x is None and y is None or z is None:
            return "N/A"
        if x is None:
            x = "na"
        if y is None:
            y = "na"
        return f"{x}-{y}={z//10:04}"
    df = create_dataframe(spark_session)
    df = multi_column_formatter(df, "new_column", format_fun, "string1", "string2", "number")
    answer = {"a": "hello world-goodbye=0001", "b": "-na=0010", "c": "N/A", "d": "N/A"}
    answer_check(df, answer, "new_column")

