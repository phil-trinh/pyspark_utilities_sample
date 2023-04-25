"""
Table of Contents

1. convert_dms_to_decimal: Convert DMS format to decimal degrees.
2. haversine: Compute haversine distance between pair of lat-longs.
"""

import math
from pyspark.sql import functions as F, types as T, DataFrame

from ..parsing.non_dataframe_string_fun import ensure_no_conflict


def convert_dms_to_decimal(df: DataFrame, dms_col: str, dd_col: str, is_lat: bool) -> DataFrame:
    """
    Convert DMS format to decimal degrees.

    If dms_col == dd_col, dms_col will be overwritten.

    Inputs
        df: DataFrame.
        dms_col: Name of column in degrees-minutes-seconds format.
        dd_col: Name of column to create for decimal degrees format.
        is_lat: True if the column represents latitudes, False if it represents longitude.

    Output
        Updated DataFrame with {dd_col} column added.

    Example
        df = convert_dms_to_decimal(df, "latitude", "latitude", True)
        Will overwrite the "latitude" column after parsing from DMS to decimal degrees.
    """
    # Create variables that will find the correct sub-strings based on the input being a latitude or a longitude.
    if is_lat:
        deg_len = 2
        min_start = 3
        sec_start = 5
    else:
        deg_len = 3
        min_start = 4
        sec_start = 6

    # Create names of temporary columns.
    col_names = df.columns
    temp_deg = ensure_no_conflict(col_names, "deg" + dms_col)
    temp_min = ensure_no_conflict(col_names, "min" + dms_col)
    temp_sec = ensure_no_conflict(col_names, "sec" + dms_col)

    df = (
        df
        # Get the degrees, convert to number.
        .withColumn(temp_deg, F.substring(dms_col, 1, deg_len).cast("double"))

        # Get the minutes and seconds.
        .withColumn(temp_min, F.substring(dms_col, min_start, 2).cast("double"))
        .withColumn(temp_sec, F.substring(dms_col, sec_start, 2).cast("double"))

        # Divide minutes and seconds by 60 and 3600.
        .withColumn(temp_min, F.col(temp_min) / 60)
        .withColumn(temp_sec, F.col(temp_sec) / 3600)

        # Make the new column adding degrees, minutes, and seconds.
        .withColumn(dd_col, F.col(temp_deg) + F.col(temp_min) + F.col(temp_sec))

        # Be sure to include whether it needs to be positive or negative based on direction.
        .withColumn(
            dd_col,
            # Positive values for N and E.
            F.when(F.col(dms_col).contains("N"), F.col(dd_col))
            .when(F.col(dms_col).contains("E"), F.col(dd_col))
            # Negative values for S and W.
            .when(F.col(dms_col).contains("S"), F.lit(-1) * F.col(dd_col))
            .when(F.col(dms_col).contains("W"), F.lit(-1) * F.col(dd_col))
            # Unknown directions will be set to null.
            .otherwise(F.lit(None).cast("double"))
        )

        # Drop temporary columns.
        .drop(temp_deg, temp_min, temp_sec)
    )

    return df


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
    Compute haversine distance between pair of lat-longs.

    Inputs
        df: DataFrame.
        dist_col: Column to add.
        lat1_col: Name of column containing latitude of point 1, in decimal degree notation.
        lon1_col: Name of column containing longitude of point 1, in decimal degree notation.
        lat2_col: Name of column containing latitude of point 2, in decimal degree notation.
        lon2_col: Name of column containing longitude of point 2, in decimal degree notation.
        as_miles: Returns haversine distance as miles if True and as kilometers if False.

    Output
        DataFrame updated with {dist_col} column added.

    Example
        df = haversine(df, "distance_miles", "lat1", "lon1", "lat2", "lon2")
        Will compute the haversine distance between points identified in the four columns;
            the result will be stored in "distance_miles".
    """
    @F.udf(T.DoubleType())
    def _haversine_udf(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Compute haversine distance between pair of lat-longs.

        Inputs
            lat1: Latitude of point 1.
            lon1: Longitude of point 1.
            lat2: Latitude of point 2.
            lon2: Longitude of point 2.

        Output
            Distance, in miles, between the two points.

        Example
            dist = _haversine(0, 0, -24, 32)
            Will return 2709.8021 [miles].
        """

        # Do not attempt to convert nulls.
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return None

        # Difference in latitude and longitude, plus convert to radians from degrees.
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        lat1 = math.radians(lat1)
        lat2 = math.radians(lat2)

        # Haversine formula.
        under_root = math.sin(dLat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dLon / 2) ** 2
        calc = 2 * math.asin(math.sqrt(under_root))

        # Arithmetic Mean Radius of Earth in miles.
        # Source: Wikipedia contributors. (2022, July 8). Earth radius. In Wikipedia, The Free Encyclopedia.
        # Retrieved 17:05, July 26, 2022, from https://en.wikipedia.org/w/index.php?title=Earth_radius&oldid=1097124279
        radius = 3958.7613

        # Convert to kilometers if requested.
        if not as_miles:
            radius = 6371.0088

        return radius * calc

    # Call UDF to add column.
    df = df.withColumn(dist_col, _haversine_udf(F.col(lat1_col), F.col(lon1_col), F.col(lat2_col), F.col(lon2_col)))

    return df

