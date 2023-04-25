"""
Test the get_distinct_list function in parsing/array_fun.py.

get_distinct_list(df: DataFrame, column_map: dict, default_name: str = "{0}") -> DataFrame
"""

from pyspark.sql import functions as F, types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.array_fun import get_distinct_list


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
        T.StructField("an_array", T.ArrayType(T.IntegerType()), True)
    ])

    DATA = [
        ["a", [1, 2, 3, 4, 5]],
        ["b", [1, 1, 1, 2, 3]],
        ["c", [None, None, None]],
        ["d", None],
     ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_get_distinct_list_0(spark_session) -> None:
    """
    Test the function: create new column to ensure current one is unchanged; default StringType().

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    # Make copy to check these values do not change.
    df = df.withColumn("new_column", F.col("an_array"))

    # Get distinct elements of the copied column.
    df = get_distinct_list(df, "new_column")

    answer_current = {"a": [1, 2, 3, 4, 5], "b": [1, 1, 1, 2, 3], "c": [None, None, None], "d": None}
    answer_check(df, answer_current, "an_array", "current column check")

    answer_new = {"a": ["1", "2", "3", "4", "5"], "b": ["1", "2", "3"], "c": [None], "d": None}
    answer_check(df, answer_new, "new_column", "new_column check")


def test_get_distinct_list_1(spark_session) -> None:
    """
    Test the function: use IntegerType().

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = get_distinct_list(df, {"an_array": T.IntegerType()})

    answer = {"a": [1, 2, 3, 4, 5], "b": [1, 2, 3], "c": [None], "d": None}
    answer_check(df, answer, "an_array")

