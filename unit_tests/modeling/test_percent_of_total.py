"""
Test the percent_of_total function in modeling/cumulative.py.

percent_of_total(
    df: DataFrame,
    columns: [dict, list, str],
    group_by_col: [str, list] = None,
    default_name: str = "{0}_percent_of_total"
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.cumulative import percent_of_total


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
        T.StructField("grouping", T.StringType(), True),
        T.StructField("data", T.IntegerType(), True),
        T.StructField("data2", T.IntegerType(), True),
    ])

    DATA = [
        ["a", "group1", 2, 5],
        ["b", "group2",  0, 0],
        ["c", "group1",  8, 10],
        ["d", "group1",  10, 0],
        ["e", "group2", 20, None],
        ["f", "group2", None, 25]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_percent_of_total_0(spark_session) -> None:
    """
    Test the function: dict input and group by.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percent_of_total(df, {"data": "output"}, "grouping")
    answer = {"a": 0.1, "b": 0.0, "c": 0.4, "d": 0.5, "e": 1.0, "f": None}
    answer_check(df, answer, "output")


def test_percent_of_total_1(spark_session) -> None:
    """
    Test the function: dict input, no group by.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percent_of_total(df, {"data": "output"})
    answer = {"a": 0.05, "b": 0.0, "c": 0.2, "d": 0.25, "e": 0.5, "f": None}
    answer_check(df, answer, "output")


def test_percent_of_total_2(spark_session) -> None:
    """
    Test the function: dict input, no group by, multi columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percent_of_total(df, {"data": "output", "data2": "output2"})
    answer = {"a": 0.05, "b": 0.0, "c": 0.2, "d": 0.25, "e": 0.5, "f": None}
    answer2 = {"a": 0.125, "b": 0.0, "c": 0.25, "d": 0.0, "e": None, "f": 0.625}
    answer_check(df, answer, "output", "test_2_first_output")
    answer_check(df, answer2, "output2", "test_2_second_output")


def test_percent_of_total_3(spark_session) -> None:
    """
    Test the function: list of inputs, no group by, default new name.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percent_of_total(df, ["data", "data2"])
    answer = {"a": 0.05, "b": 0.0, "c": 0.2, "d": 0.25, "e": 0.5, "f": None}
    answer2 = {"a": 0.125, "b": 0.0, "c": 0.25, "d": 0.0, "e": None, "f": 0.625}
    answer_check(df, answer, "data_percent_of_total", "test_3_first_output")
    answer_check(df, answer2, "data2_percent_of_total", "test_3_second_output")


def test_percent_of_total_4(spark_session) -> None:
    """
    Test the function: list of inputs, no group by, override default new name.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percent_of_total(df, ["data", "data2"], default_name="percent_{0}")
    answer = {"a": 0.05, "b": 0.0, "c": 0.2, "d": 0.25, "e": 0.5, "f": None}
    answer2 = {"a": 0.125, "b": 0.0, "c": 0.25, "d": 0.0, "e": None, "f": 0.625}
    answer_check(df, answer, "percent_data", "test_4_first_output")
    answer_check(df, answer2, "percent_data2", "test_4_second_output")

