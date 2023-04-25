"""
Table of Contents

1. percentiles: Calculate percentiles of specified columns.
"""

from pyspark.sql import functions as F, DataFrame


def percentiles(
    df: DataFrame,
    percentiles: list,
    group_col: str,
    agg_cols: [list, str],
    new_col_name: str = "percentile"
) -> DataFrame:
    """
    Calculate percentiles of specified columns.

    Inputs
        df: DataFrame containing column to calculate percentiles from.
        percentiles: List, of integers only, of percentiles to calculate.
            NOTE: Percentile calculations are inclusive, meaning the output captures the value
                  at or below which a given percentage falls.
        group_col: Group by column name from calc_df and pivot_df.
        agg_col: Name as string or list of strings of column(s) from calc_df to calculate percentiles from.
        new_col_name: Description to add to the front of the new column. Columns added will also append the percent
                      being calculated and the name of the column used as input as {new_col_name}_{percent}_{agg_col}.

    Output
        New DataFrame containing the percentiles of the specified columns.

    Example
        pivot = percentiles(df, [25, 75], "group", "agg_name")
        Will return a new DataFrame with columns percentile_25_agg_name and percentile_75_agg_name.
    """
    # List or string allowed, ensure a list is used.
    if isinstance(agg_cols, str):
        agg_cols = [agg_cols]

    # Set first = True to itterate on first column
    first = True
    for col in agg_cols:
        for perc in percentiles:
            # Create new column name based on calculated percentile(s) and data column name.
            new_name = "{0}_{1}_{2}".format(new_col_name, perc, col)

            # Pivot df and group_by "group_col" to create percentile_pivot using aggregation and expression.
            percentile_pivot = (
                df
                .groupby(group_col)
                .agg(F.expr(f"percentile({col}, array(0.{perc}))")[0].alias(new_name))
            )

            # If the first calculation, store. Otherwise, use the group-by columns to join previous calculations to the
            # current one.
            if first:
                output = percentile_pivot
                first = False
            else:
                output = output.join(percentile_pivot, how='left', on=group_col)

    return output

