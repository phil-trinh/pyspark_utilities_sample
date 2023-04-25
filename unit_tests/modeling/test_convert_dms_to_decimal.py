"""
Test the convert_dms_to_decimal function in modeling/geolocation.py.

convert_dms_to_decimal(df: DataFrame, dms_col: str, dd_col: str, is_lat: bool) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.geolocation import convert_dms_to_decimal

TOL = 0.000001  # Tolerance level when comparing float values.


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
        T.StructField("DMS_Lat", T.StringType(), True),
        T.StructField("DMS_Lon", T.StringType(), True),
    ])

    DATA = [
        ["a", "533036N", "1003036E"],
        ["b", "103036S", "0103036W"],
        ["c", "303036N", "0101010W"],
        ["d", "533036S", "0103036E"],
        ["e", None, None],
        ["f", "800000A", "1000000B"],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_convert_dms_to_decimal_0(spark_session) -> None:
    """
    Test the function: test latitude column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = convert_dms_to_decimal(df, "DMS_Lat", "DD_Lat", is_lat=True)
    answer = {"a": 53.51, "b": -10.51, "c": 30.51, "d": -53.51, "e": None, "f": None}
    answer_check(df, answer, "DD_Lat", tol=TOL)


def test_convert_dms_to_decimal_1(spark_session) -> None:
    """
    Test of the function: test longitude column.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = convert_dms_to_decimal(df, "DMS_Lon", "DD_Lon", is_lat=False)
    answer = {"a": 100.51, "b": -10.51, "c": -10.1694444, "d": 10.51, "e": None, "f": None}
    answer_check(df, answer, "DD_Lon", tol=TOL)

