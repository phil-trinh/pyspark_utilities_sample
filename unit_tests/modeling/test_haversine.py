"""
Test the Haversine function in modeling/geolocation.py.

def haversine(
    df: DataFrame,
    dist_col: str,
    lat1_col: str,
    lon1_col: str,
    lat2_col: str,
    lon2_col: str,
    as_miles: bool = True
) -> DataFrame:
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.modeling.geolocation import haversine

TOL = 0.001


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
        T.StructField("Lat_1", T.IntegerType(), True),
        T.StructField("Long_1", T.IntegerType(), True),
        T.StructField("Lat_2", T.IntegerType(), True),
        T.StructField("Long_2", T.IntegerType(), True)
    ])

    DATA = [
        ["a", 0, 0, -24, 32],
        ["b", 1, 1, 20, 8],
        ["c", None, 55, 20, 2],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_haversine_0(spark_session) -> None:
    """
    Test the function: return haversine distance between two points in miles as a float.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = haversine(df, "haversine_distance", "Lat_1", "Long_1", "Lat_2", "Long_2")

    answer = {
        "a": 2709.8021,
        "b": 1395.4314,
        "c": None,
    }

    answer_check(df, answer, "haversine_distance", tol=TOL)


def test_haversine_1(spark_session) -> None:
    """
    Test the function: return haversine distance between two points in kilometers as a float.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = haversine(df, "haversine_distance", "Lat_1", "Long_1", "Lat_2", "Long_2", as_miles=False)

    answer = {
        "a": 4361.0038,  # Rounded answer verified by http://www.movable-type.co.uk/scripts/latlong.html
        "b": 2245.7292,  # Rounded answer verified by http://www.movable-type.co.uk/scripts/latlong.html
        "c": None,
    }

    answer_check(df, answer, "haversine_distance", tol=TOL)

