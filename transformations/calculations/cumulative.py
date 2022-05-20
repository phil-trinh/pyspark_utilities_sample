"""
Table of Contents

1. cumul_sum: Calculate cumulative sum for target columns.
2. percent_of_total: Calculate the percent of the total of a column.
"""

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window

from ..library_utils import str_or_list_to_dict


def cumul_sum(
    df: DataFrame,
    columns: [dict, list, str],
    cols_order: [list, str],
    cols_grp: [list, str] = None,
    default_name: str = "{0}_cumul_sum"
) -> DataFrame:
    """
    Calculate cumulative sum for target columns.

    Inputs
        df: DataFrame.
        columns: Dictionary of columns of which to compute the cumulative sum.
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        cols_order: List of columns to order by.
        cols_grp: List of columns to partition/group by.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.

    Output
        DataFrame with cumulative columns added.

    Example
        df = cumul_sum(df, {"col1": "col1_cumulative"}, ["order_col"])
        Will add the column col1_cumulative by ordering by order_col and calculating the cumulative sum of col1.

        df = cumul_sum(df, "col1", default_name = "cumulative_{0}")
        Will add the column cumulative_col1 when calculating the cumulative sum of col1.
    """
    # Default None for cols_grp is not valid, make it an empty list.
    if cols_grp is None:
        cols_grp = []

    # Create Window based on the group-by and order-by columns.
    window_spec = Window.partitionBy(cols_grp).orderBy(cols_order).rangeBetween(Window.unboundedPreceding, 0)

    # Allowing for a single string or a list. Make sure it is a dictionary.
    columns = str_or_list_to_dict(columns, default_name)

    for col_name in columns:
        df = df.withColumn(columns[col_name], F.sum(F.col(col_name)).over(window_spec))

    return df


def percent_of_total(
    df: DataFrame,
    columns: [dict, list, str],
    group_by_col: [str, list] = None,
    default_name: str = "{0}_percent_of_total"
) -> DataFrame:
    """
    Calculate the percent of the total of a column.

    Inputs
        df: DataFrame.
        columns: Dictionary of columns of which to compute the percent of the total.
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        group_by_col: Column that will group the values in the existing column.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.

    Output
        Dataframe with new columns that calculate the percent of the total.

    Example
        df = percent_of_total(df, "existing_column", group_by_col="grouping")
        Will compute the percent that each row of numbers (IntegerType, DoubleType) contribute to the group's total in a
            new column named existing_column_percent_of_total.

        df = percent_of_total(df, {"existing": "new"})
        Will compute the sum of the "existing" column and compute each row's contribution in "new".
    """
    if group_by_col is None:
        group_by_col = []

    # Define Window w so that we sum by groups.
    w = Window.partitionBy(group_by_col)

    # Input is allowed to be a string, list, or a dictionary; ensure the code only deals with dictionaries.
    columns = str_or_list_to_dict(columns, default_name)

    for col in columns:
        # Get the total of the column after grouping.
        df = df.withColumn(columns[col], F.sum(col).over(w))
        # Get the percent of the newly calculated total.
        df = df.withColumn(columns[col], F.col(col) / F.col(columns[col]))

    return df
