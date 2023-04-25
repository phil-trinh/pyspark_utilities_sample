"""
Test the standard_case_column function in parsing/schema_ops.py.

standard_case_columns(df: DataFrame, as_upper: bool = True) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import assertion_helper

from transformations_library.parsing.schema_ops import standard_case_columns


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session - Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """

    SCHEMA = T.StructType([
        T.StructField("Column1", T.StringType(), True),
        T.StructField("column2", T.StringType(), True),
        T.StructField("COLUMN3", T.StringType(), True),
        T.StructField("CoLuMn4", T.StringType(), True),
    ])

    DATA = [
        [None, None, None, None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_standard_case_column_0(spark_session) -> None:
    """
    Test of the function: Lower case.

    Input
        spark_session - Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = standard_case_columns(df, as_upper=False)
    answer = set(["column1", "column2", "column3", "column4"])
    compare_to = set(df.columns)
    assertion_helper(compare_to, answer, f"Computed {compare_to} but expected {answer}.")


def test_standard_case_column_1(spark_session) -> None:
    """
    Test of the function: Upper case.

    Input
        spark_session - Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = standard_case_columns(df, as_upper=True)
    answer = set(["COLUMN1", "COLUMN2", "COLUMN3", "COLUMN4"])
    compare_to = set(df.columns)
    assertion_helper(compare_to, answer, f"Computed {compare_to} but expected {answer}.")

