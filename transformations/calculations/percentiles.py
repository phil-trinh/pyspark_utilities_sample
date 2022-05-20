"""
Table of Contents

1. percentiles: For a list of percentiles, calculate percentiles and join with pivoted DataFrame.
"""

from pyspark.sql import functions as F, DataFrame


def percentiles(
    pivot_df: DataFrame,
    calc_df: DataFrame,
    percentiles: list,
    group_col: str,
    agg_cols: [list, str],
    new_col_name: str = "percentile"
) -> DataFrame:
    """
    For a list of percentiles, calculate percentiles and join with pivoted DataFrame.

    Inputs
        pivot_df: Pivot table to join percentiles to.
        calc_df: DataFrame containing column to calculate percentiles from.
        percentiles: List, of integers only, of percentiles to calculate.
        group_col: Group by column name from calc_df and pivot_df.
        agg_col: Name as string or list of strings of column(s) from calc_df to calculate percentiles from.
        new_col_name: Description to add to the front of the new column. Columns added will also append the percent
                      being calculated and the name of the column used as input as {new_col_name}_{percent}_{agg_col}.

    Output
        Updated Pivot DataFrame.

    Example
        pivot = percentiles(pivot, calc, [25, 75], "group", "agg_name")
        Will add columns percentile_25_agg_name, and percentile_75_agg_name to pivot by joining on "group".
    """
    # List or string allowed, ensure a list is used.
    if isinstance(agg_cols, str):
        agg_cols = [agg_cols]

    for col in agg_cols:
        for perc in percentiles:
            new_name = "{0}_{1}_{2}".format(new_col_name, perc, col)
            percentile_pivot = (
                calc_df
                .groupby(group_col)
                .agg(F.expr(f"percentile({col}, array(0.{perc}))")[0].alias(new_name))
            )

            pivot_df = pivot_df.join(percentile_pivot, how='left', on=group_col)

    return pivot_df
