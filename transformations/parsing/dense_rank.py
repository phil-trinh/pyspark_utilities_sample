"""
Table of Contents

1. general_ranking: Add a column to rank a dataset as ordered over one or more columns, by rank() or dense_rank().
2. dense_ranking: Add a column to densely rank the values by descending order of a column.
"""

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window


def general_ranking(
    df: DataFrame,
    columns: [list, str],
    rank_col_name: str = "rank",
    use_rank_function: bool = True,
    ascending: bool = True
) -> DataFrame:
    """
    Add a column to rank a dataset as ordered over one or more columns, by rank() or dense_rank().
    Note: nulls are sorted last regardless of other input options.

    Inputs
        df: DataFrame.
        columns: Name of existing column(s) containing the values to rank.
                 Three input styles are allowed. Use a string or a list of strings along with the ascending keyword
                 argument to set the sorting direction for all columns given. Or, provide a list of lists such that each
                 nested list is of the form ["column_name", True/False]; here, each column will set its own ascending
                 parameter value - note the ascending keyword argument will be ignored for this case.
        rank_col_name: Name of column to create.
        use_rank_function: True to use the rank() method, False to use dense_rank().
        ascending: Order ascending if True, descending if False.

    Output
        Updated DataFrame with rank column added.

    Example
        df = general_ranking(df, [["date1_col", True], ["date2_col", False]], "date_rank", False)
        Will sort the values of "date1_col" in ascending order first and then sort date2_col in descending order,
        and create a dense ranking in the new date_rank column.
    """
    # If a string is provided, wrap as a list.
    if isinstance(columns, str):
        columns = [columns]

    # If columns is a list of strings, convert to a list of lists, to customize the sorting direction.
    if isinstance(columns[0], str):
        columns = [[col, ascending] for col in columns]

    # Create the order-by columns.
    orderby_cols = []
    for (col, asc) in columns:
        if asc:  # True to sort in ascending order.
            orderby_cols.append(F.col(col).asc_nulls_last())
        else:  # False to sort in descending order
            orderby_cols.append(F.col(col).desc_nulls_last())

    # Create Window based on the order-by columns.
    window_spec = Window.orderBy(*orderby_cols)

    if use_rank_function:
        df = df.withColumn(rank_col_name, F.rank().over(window_spec))
    else:
        df = df.withColumn(rank_col_name, F.dense_rank().over(window_spec))

    return df


def dense_ranking(df: DataFrame, column: [list, str], rank_col_name: str = "rank") -> DataFrame:
    """
    Add a column to densely rank the values by descending order of a column.

    Inputs
        df: DataFrame.
        column: Name of existing column, or list of column names, containing the values to rank.
        rank_col_name: Name of column to create.

    Output
        Updated DataFrame with rank column added.

    Example
        df = dense_ranking(df, "date_col", "date_rank")
        Will sort the values of "date_col" in descending order and create a dense ranking in the new date_rank column.
    """
    return general_ranking(df, column, rank_col_name, False, False)

