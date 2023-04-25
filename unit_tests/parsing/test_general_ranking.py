"""
Test the general_ranking function in parsing/dense_rank.py.

general_ranking(
    df: DataFrame,
    columns: [list, str],
    rank_col_name: str = "rank",
    use_rank_function: bool = True,
    ascending: bool = True
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame
from datetime import datetime

from ..unit_test_utils import answer_check

from transformations_library.parsing.dense_rank import general_ranking


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
        ["h", datetime.strptime("02/18/2017", "%m/%d/%Y").date(), None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_general_ranking_0(spark_session) -> None:
    """
    Test the function: ordered in ascending order using default rank() function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = general_ranking(df, "date", "new_rank_col")
    answer = {"a": 1, "b": 6, "c": 5, "d": 3, "e": 3, "f": 7, "g": 7, "h": 1}
    answer_check(df, answer, "new_rank_col")


def test_general_ranking_1(spark_session) -> None:
    """
    Test the function: ordered on 2 columns in ascending order using dense_rank() function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = general_ranking(df, ["an_int", "date"], "new_rank_col", False)
    answer = {"a": 3, "b": 5, "c": 4, "d": 6, "e": 2, "f": 8, "g": 1, "h": 7}
    answer_check(df, answer, "new_rank_col")


def test_general_ranking_2(spark_session) -> None:
    """
    Test the function: ordered in descending order on 2 columns using default rank() function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = general_ranking(df, ["date", "an_int"], "new_rank_col", ascending=False)
    answer = {"a": 5, "b": 1, "c": 2, "d": 3, "e": 4, "f": 8, "g": 7, "h": 6}
    answer_check(df, answer, "new_rank_col")


def test_general_ranking_3(spark_session) -> None:
    """
    Test the function: ordered on two columns, one ascending and one descending, using default rank() function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = general_ranking(df, [["date", True], ["an_int", False]])
    answer = {"a": 1, "b": 6, "c": 5, "d": 3, "e": 4, "f": 8, "g": 7, "h": 2}
    answer_check(df, answer, "rank")

