"""
Table of Contents

1. drop_null_columns: Drop columns that exceed a threshold of null values.
"""

from pyspark.sql import functions as F, DataFrame

from ..parsing.melting import melt


def drop_null_columns(df: DataFrame, threshold: float = 1.0, subset: list = None) -> DataFrame:
    """
    Drop columns that exceed a threshold of null values.

    Inputs
        df: DataFrame.
        threshold: Threshold value. If a column has at least this fraction of nulls, it will be removed.
        subset: List of columns to check. All others will be kept by default regardless of the null count.

    Output
        Updated DataFrame.

    Example
        df = df.drop_null_columns(df, 0.5, ["col1", "col2"])
        Will remove col1 or col2 if either is at least 50% null.
    """
    # If looking only at a subset of columns, downselect, otherwise, evaluate all columns.
    if subset is None:
        temp_df = df
    else:
        temp_df = df.select(subset)

    # List of columns being evaluated.
    columns_evaluated = temp_df.columns

    # Replace each column with a 1 if null, 0 otherwise.
    for col in columns_evaluated:
        temp_df = temp_df.withColumn(col, F.when(F.col(col).isNull(), 1).otherwise(0))

    # Sum the number of nulls, represented with a 1, for each column.
    null_count = temp_df.agg(*[F.sum(c).alias(c) for c in columns_evaluated])

    # Total row count.
    count = temp_df.count()

    # Pivot this null_count DataFrame to do a few operations to find when there are too many nulls.
    # The names of the columns will now be listed in a column called "categories".
    null_count = melt(null_count, value_vars=columns_evaluated)
    null_count = (
        null_count
        # Compute the fraction of rows that are null.
        .withColumn("fraction", F.col("values") / F.lit(count))
        # Keep those rows that meet or exceed the threshold.
        .where(F.col("fraction") >= F.lit(threshold))
        # Now, pivot again so that the column names are restored.
        .groupBy().pivot("categories").sum("fraction")
    )

    # Get the list of the columns that need to be dropped; drop and return.
    columns_to_drop = null_count.columns
    df = df.drop(*columns_to_drop)

    return df

