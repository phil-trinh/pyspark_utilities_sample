"""
Table of Contents

1. change_name_format: Splits names in last.first format to Last, First format.
"""

from pyspark.sql import functions as F, DataFrame

from .non_dataframe_string_fun import ensure_no_conflict
from ..library_utils import str_or_list_to_dict


def change_name_format(
    df: DataFrame,
    columns: [str, list, dict],
    sep: str = "\\.",
    default_name: str = "{0}"
) -> DataFrame:
    """
    Splits names in last.first format to Last, First format.

    Input
        df: DataFrame.
        columns: Dictionary of columns of which to split.
                 Key represents the name of the column to use.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        sep: Name delimiter separating last and first name. Assumed to be . (period) which requires \\ in front.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.

    Output
        DataFrame with columns updated to new format.

    Example
        df = change_name_format(df, "name")
        Will overwrite the "name" column with the formatting changed from last.first to Last, First.
    """
    # Hat tip to https://stackoverflow.com/questions/11457236/string-split-returning-null-when-using-a-dot/11457253
    # Allowing for a single string or a list. Make sure it is a dictionary.
    columns = str_or_list_to_dict(columns, default_name)

    # Using a temporary column for last and first names; ensure they do not conflict with an existing column.
    temp_last_name = ensure_no_conflict(df.columns, "last_name")
    temp_first_name = ensure_no_conflict(df.columns, "first_name")

    for col in columns:
        df = (
            df
            # Last name is first entry (index 0) before the separator. Capitalize the first letter.
            .withColumn(temp_last_name, F.split(F.col(col), sep).getItem(0).alias('last'))
            .withColumn(temp_last_name, F.initcap(F.col(temp_last_name)))

            # First name is second entry (index 1) before the separator. Capitalize the first letter.
            .withColumn(temp_first_name, F.split(F.col(col), sep).getItem(1).alias('first'))
            .withColumn(temp_first_name, F.initcap(F.col(temp_first_name)))

            # Combine names as Last, First and drop temporary columns.
            .withColumn(columns[col], F.concat_ws(', ', F.col(temp_last_name), F.col(temp_first_name)))
            .drop(*[temp_first_name, temp_last_name])
        )

    return df

