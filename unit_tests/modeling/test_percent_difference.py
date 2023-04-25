"""
Test the percent difference function in modeling/math_percent_expressions.py.

percent_diff(
    df: DataFrame,
    percent_diff_col: str,
    col_1: str,
    col_2: str
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.math_percent_expressions import percent_difference

TOL = 0.0001


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
        T.StructField("Col_1", T.IntegerType(), True),
        T.StructField("Col_2", T.IntegerType(), True),
    ])

    DATA = [
        ["a", 5, 10],
        ["b", 20, 5],
        ["c", 0, 0],
        ["d", None, 5],
        ["e", 0, 5],
        ["f", 1, -1],
        ["g", -5, -10],
        ["h", 6, 6],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_percent_difference_0(spark_session) -> None:
    """
    Test the function: return the percent difference to the nearest hundredth.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percent_difference(df, "percent_difference", "Col_1", "Col_2")

    answer = {
        "a": 0.66666,
        "b": 1.2,
        "c": None,
        "d": None,
        "e": 2,
        "f": None,
        "g": 0.66666,
        "h": 0,
    }

    answer_check(df, answer, "percent_difference", tol=TOL)

