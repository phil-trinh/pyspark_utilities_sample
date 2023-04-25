"""
Test the percent change function in modeling/math_percent_expressions.py.

percent_change(
    df: DataFrame,
    percent_change_col: str,
    init_col: str,
    final_col: str,
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.math_percent_expressions import percent_change

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
        T.StructField("initial_col", T.IntegerType(), True),
        T.StructField("final_col", T.IntegerType(), True),
    ])

    DATA = [
        ["a", 60, 95],
        ["b", 5, 20],
        ["c", None, 5],
        ["d", 20, 5],
        ["e", 0, 5],
        ["f", -5, 100],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_percent_change_0(spark_session) -> None:
    """
    Test the function: return the percent change to the nearest hundredth.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = percent_change(df, "percent_change", "initial_col", "final_col")

    answer = {
        "a": 0.58333,
        "b": 3,
        "c": None,
        "d": 0.75,
        "e": None,
        "f": 21,
    }

    answer_check(df, answer, "percent_change", tol=TOL)

