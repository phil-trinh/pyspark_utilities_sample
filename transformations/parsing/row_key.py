"""
Table of Contents

1. primary_key: Add a primary key column.
2. row_number_key: Create unique key column based on row numbers ordered by a column.
"""

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window


def primary_key(df: DataFrame, *columns: str, key_col: str = "key", sep: str = "-") -> DataFrame:
    """
    Add a primary key column by concatenating values from other columns.

    Inputs
        df: DataFrame.
        columns: Variable number of column names to concatenate for the key.
        key_col: Name of primary key column to add. Must be given as key_col = "...".
        sep: Value delimiter when joining columns. Must be given as sep = "...".

    Output
        Updated DataFrame with the key column added.

    Example
        df = primary_key(df, "col1", "col2", "col3", key_col="primary")
        Will add the column "primary" as a concatenation of the values from col1, col2, col3, separated with the
        default of sep='-'; i.e., col1-col2-col3.
    """

    return df.withColumn(key_col, F.concat_ws(sep, *[F.col(c).cast("string") for c in columns]))


def row_number_key(df: DataFrame, ordering_column: str, num_col: str = "row_number", as_str: bool = True) -> DataFrame:
    """
    Create unique key column based on row numbers ordered by a column.

    Inputs
        df: DataFrame.
        ordering_column: Column name to orderby for windowing function.
        num_col: Name of new column to add.
        as_str: True to convert row numbers to StringType; False to leave as IntegerType.

    Output
        Dataframe with unique key column with row numbers.

    Example
        df = row_number_key(df, "col1", as_str = False)
        Will add a "row_number" IntegerType column to DataFrame df.
    """
    # Two options for the data type, strings or integers. Pick the cast.
    if as_str:
        casting = "string"
    else:
        casting = "int"

    return df.withColumn(num_col, F.row_number().over(Window.orderBy(ordering_column)).cast(casting))

