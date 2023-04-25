"""
Test the dense_ranking function in parsing/dense_rank.py.

dense_ranking(df: DataFrame, column: [list, str], rank_col_name: str = "rank") -> DataFrame
"""

from pyspark.sql import types as T, DataFrame
from datetime import datetime

from ..unit_test_utils import answer_check

from transformations_library.parsing.dense_rank import dense_ranking


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
        T.StructField("date", T.DateType(), True),
        T.StructField("an_int", T.IntegerType(), True),
    ])

    DATA = [
        ["a", datetime.strptime("02/18/2017", "%m/%d/%Y").date(), 12],
        ["b", datetime.strptime("11/17/2021", "%m/%d/%Y").date(), 33],
        ["c", datetime.strptime("09/30/2020", "%m/%d/%Y").date(), 33],
        ["d", datetime.strptime("09/03/2020", "%m/%d/%Y").date(), 42],
        ["e", datetime.strptime("09/03/2020", "%m/%d/%Y").date(), 8],
        ["f", None, None],
        ["g", None, 1],
     ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_dense_ranking_0(spark_session) -> None:
    """
    Test the function: sort on date column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = dense_ranking(df, "date", "new_rank_col")
    answer = {"a": 4, "b": 1, "c": 2, "d": 3, "e": 3, "f": 5, "g": 5}
    answer_check(df, answer, "new_rank_col")


def test_dense_ranking_1(spark_session) -> None:
    """
    Test the function: sort on int column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = dense_ranking(df, "an_int", "new_rank_col")
    answer = {"a": 3, "b": 2, "c": 2, "d": 1, "e": 4, "f": 6, "g": 5}
    answer_check(df, answer, "new_rank_col")


def test_dense_ranking_2(spark_session) -> None:
    """
    Test the function: sort on both the date and the int column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = dense_ranking(df, ["date", "an_int"], "new_rank_col")
    answer = {"a": 5, "b": 1, "c": 2, "d": 3, "e": 4, "f": 7, "g": 6}
    answer_check(df, answer, "new_rank_col")

