"""
Table of Contents

1. forward_fill: Perform a forward fill on a column.
2. cartesian_product: Ensure all combinations of values are present from multiple columns.
"""

from pyspark.sql import functions as F, types as T, DataFrame, SparkSession
from pyspark.sql.window import Window

from ..library_params import SPARK_TYPE_MAP
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


def cartesian_product(df: DataFrame, columns: list = None, values: dict = None) -> DataFrame:
    """
    Ensure all combinations of values are present from multiple columns.

    Inputs
        df: DataFrame.
        columns: List of columns from the DataFrame from which to find distinct values.
        values: Dictionary mapping a current column to a list of all desired entries needed.

    Output
        Updated DataFrame with each pairing given from the specified columns.

    Example
        df = cartesian_product(df, ["col_a", "col_b"], {"col_c": [1, 2]})
        Will pair all existing values individually found in columns col_a and col_b, and then combine that with the
            values 1 and 2 in column col_c. Here no values not already found in col_a and col_b will be added, but all
            pairs of (col_a x col_b) will have values 1 and 2 in col_c - even if one of those values never appears in
            the original DataFrame.
    """
    # Original order of columns to prevent shuffling.
    column_order = df.columns

    # Original column types.
    column_types = dict(df.dtypes)

    if columns is not None:
        # Find the unique values of the first column.
        full = df.select(columns[0]).distinct()
        for col in columns[1:]:
            # For subsequent columns, cross join to get the cartesian product.
            this_col = df.select(col).distinct()
            full = full.crossJoin(this_col)
    else:
        columns = []
        full = None

    # If default value of None are given, use an empty dictionary, effectively skipping this section.
    if values is None:
        values = {}

    # Create a Spark Session to build a DataFrame containing the specified values for the column.
    spark_session = SparkSession.builder.getOrCreate()

    # Create a simple data frame with every value provided by the user.
    for col in values:
        # Build the DataFrame containing just one column of the specified entries, using the schema from the existing
        # DataFrame.
        data = [[x] for x in values[col]]
        schema = T.StructType([T.StructField(col, SPARK_TYPE_MAP[column_types[col]], True)])
        this_col = spark_session.createDataFrame(data=data, schema=schema)

        # Perform a cross join of this simple DF with the previous cartesian product DF.
        if full is not None:
            full = full.crossJoin(this_col)
        else:
            full = this_col

        # Add this particular column as a join key.
        columns.append(col)

    # Pull in data from the original DF. Data should not be dropped - unless the columns provided as a join key do not
    # sufficiently handle all possible combinations desired.
    full = full.join(df, on=columns, how="outer")

    # Reset the order after the join.
    full = full.select(column_order)

    return full

