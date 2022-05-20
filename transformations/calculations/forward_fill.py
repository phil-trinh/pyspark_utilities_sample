"""
Table of Contents

1. forward_fill: Perform a forward fill on a column.
"""

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window

from ..library_utils import str_or_list_to_dict


# hat tip to https://johnpaton.net/posts/forward-fill-spark/
def forward_fill(
    df: DataFrame,
    columns: [dict, list, str],
    ordering: str,
    partitioning: [list, str] = None,
    default_name: str = "{0}"
) -> DataFrame:
    """
    Perform a forward fill on a column.

    Missing values are forward-filled from last known (non-null) data point.

    Inputs:
        df: DataFrame.
        columns: Dictionary of columns of which to forward fill.
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        ordering: Name of column which can be sorted and contains all time-steps.
        partitioning: List of columns by which to partition the data.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.

    Output
        DataFrame with columns filled in with last non-null value.

    Example
        df = forward_fill(df, ["col1", "col2"], "order_by", "group_by")
        Will fill both col1 and col2, overwritting each one, after ordering and grouping by the respective inputs.
    """
    if partitioning is None:
        partitioning = []
    elif isinstance(partitioning, str):
        partitioning = [partitioning]

    # Define the window.
    window = Window.partitionBy(partitioning).orderBy(ordering).rowsBetween(Window.unboundedPreceding, 0)

    # Allowing for a single string or a list. Make sure it is a dictionary.
    columns = str_or_list_to_dict(columns, default_name)

    for col in columns:
        # Create a new column so that the existing one is unchanged.
        df = df.withColumn(columns[col], F.col(col))

        # Do the forward fill.
        df = df.withColumn(columns[col], F.last(F.col(columns[col]), ignorenulls=True).over(window))

    return df
