"""
Table of Contents

1. percent_change: Compute percent change between two numerical values.
2. percent_difference: Compute percent difference between two numerical values.
"""

import math
from pyspark.sql import functions as F, types as T, DataFrame


def percent_change(
    df: DataFrame,
    percent_change_col: str,
    init_col: str,
    final_col: str,
) -> DataFrame:
    """
    Compute percent change between two numerical values.

    Inputs
        df: DataFrame.
        percent_change_col: Column to add.
        init_col: Name of the initial column.
        final_col: Name of the final column.

    Output
        DataFrame updated with percnt_change_col column added.

    Example
        df = percent_change(df, "percent_change", "Col_1", "Col_2")
        Will compute the percent change between the elements in the two columns;
            the result will be stored in "percent_change" as a decimal
    """
    @F.udf(T.DoubleType())
    def _percent_change_udf(orig_col: float, new_col: float) -> float:
        """
        Compute the percent change between two columns.

        Inputs
            orig_col: Numerical Value of initial element.
            new_col: Numerical value of new element.

        Output
            Percent change between the two columns.
        """
        # Do not attempt to calculate nulls.
        if orig_col is None or new_col is None:
            return None

        # Do not attempt to calculate original column of 0.
        if orig_col == 0:
            return None

        # Percent Change formula.
        return math.fabs(orig_col - new_col) / math.fabs(orig_col)

    # Call UDF to compute percent change.
    df = df.withColumn(percent_change_col, _percent_change_udf(F.col(init_col), F.col(final_col)))

    return df


def percent_difference(
    df: DataFrame,
    percent_diff_col: str,
    col_1: str,
    col_2: str,
) -> DataFrame:
    """
    Compute percent difference between two numerical values.

    Inputs
        df: DataFrame.
        percent_diff_col: Column to add.
        col_1: Name of the first column.
        col_2: Name of the second column.

    Output
        DataFrame updated with percnt_diff_col column added.

    Example
        df = percent_difference(df, "percent_difference", "Col_1", "Col_2")
        Will compute the percent difference between the elements in the two columns;
            the result will be stored in "percent_difference".
    """
    @F.udf(T.DoubleType())
    def _percent_difference_udf(col1: float, col2: float) -> float:
        """
        Compute the percent difference between two columns.

        Inputs
            col1: Numerical Value of column 1.
            col2: Numerical value of column 2.

        Output
            Percent difference between the two columns.
        """
        # Do not attempt to calculate nulls.
        if col1 is None or col2 is None:
            return None

        # Do not attempt to calculate if both columns add to 0.
        if col1 + col2 == 0:
            return None

        # Percent difference formula.
        denominator = math.fabs(col1 + col2) / 2
        return math.fabs(col1 - col2) / denominator

    # Call UDF to compute percent change
    df = df.withColumn(percent_diff_col, _percent_difference_udf(F.col(col_1), F.col(col_2)))

    return df

