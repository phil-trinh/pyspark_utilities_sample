"""
Test the bucket function in modeling/bucketing.py.

bucket(df: DataFrame, column_map: dict, buckets: dict) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.bucketing import bucket


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
        T.StructField("col_a", T.IntegerType(), True),
        T.StructField("col_b", T.IntegerType(), True),
    ])

    DATA = [
        ["a", 6, 15],
        ["b", -12, 36],
        ["c", None, None],
        ["d", 65, 0],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_bucket_0(spark_session) -> None:
    """
    Test the function: test [a, b) bucket style.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)

    buckets = {
        "[-20, 6.0)": "low",
        "[6.0, 15)": "medium",
        "[15, inf)": "high",
    }
    df = bucket(df, {"col_a": "col_a_bucket", "col_b": "col_b_bucket"}, buckets)

    answer_a = {"a": "medium", "b": "low", "c": None, "d": "high"}
    answer_check(df, answer_a, "col_a_bucket", extra_msg="col_a")

    answer_b = {"a": "high", "b": "high", "c": None, "d": "low"}
    answer_check(df, answer_b, "col_b_bucket", extra_msg="col_b")


def test_bucket_1(spark_session) -> None:
    """
    Test the function: test mixed interval bucket styles.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)

    buckets = {
        "(-inf, -12)": "low",
        "[-12, 15)": "medium",
        "[15, 36]": "medhi",
        "(36, inf)": "high",
    }
    df = bucket(df, {"col_a": "col_a_bucket", "col_b": "col_b_bucket"}, buckets)

    answer_a = {"a": "medium", "b": "medium", "c": None, "d": "high"}
    answer_check(df, answer_a, "col_a_bucket", extra_msg="col_a")

    answer_b = {"a": "medhi", "b": "medhi", "c": None, "d": "medium"}
    answer_check(df, answer_b, "col_b_bucket", extra_msg="col_b")

