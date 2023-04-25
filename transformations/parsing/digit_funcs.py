"""
Table of Contents

1. first_n_digits: Return the leading digit(s) of an integer.
2. last_n_digits: Return the last digit(s) of an integer.
"""

from pyspark.sql import functions as F, types as T, DataFrame

from ..library_utils import str_or_list_to_dict


def first_n_digits(
    df: DataFrame,
    columns: [dict, list, str],
    num_digits: int = 1,
    default_name: str = "{0}_leading"
) -> DataFrame:
    """
    Return the leading digit(s) of an integer.

    Inputs
        df: DataFrame.
        columns: Dictionary of columns of which to take the first n digits.
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        num_digits: Number of leading digits to take.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.

    Output
        DataFrame with new/updated column(s) of the first digit(s) added.

    Example
        df = first_digit(df, {"col": "leading_digit"})
        Will add column "leading_digit" by extracting it from "col".
    """
    @F.udf(T.IntegerType())
    def _first_udf(x: int) -> int:
        """
        Return the leading digit of the input.

        Input
            x: Integer value.

        Output
            First n digits.
        """
        # No operations on nulls.
        if x is None:
            return None

        # Ensure a positive value.
        x = abs(x)

        # Determine the first n digits.
        while x >= (10 ** num_digits):
            x //= 10

        return x

    # Various types allowed for input, ensure a dict is used in loop.
    columns = str_or_list_to_dict(columns, default_name)

    # Call UDF for each specified column.
    for col in columns:
        df = df.withColumn(columns[col], _first_udf(F.col(col)))

    return df


def last_n_digits(
    df: DataFrame,
    columns: [dict, list, str],
    num_digits: int = 1,
    default_name: str = "{0}_trailing"
) -> DataFrame:
    """
    Return the last digit(s) of an integer.

    Inputs
        df: DataFrame.
        columns: Dictionary of columns of which to take the last digit(s).
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.

    Output
        DataFrame with new columns added of the last digit(s).

    Example
        df = last_digit(df, {"col": "last_digit"})
        Will add column "last_digit" by extracting it from "col".
    """
    # Create UDF to get the last digit.
    last_udf = F.udf(lambda x: abs(x) % (10 ** num_digits) if x is not None else None, T.IntegerType())

    # Various types allowed for input, ensure a dict is used in loop.
    columns = str_or_list_to_dict(columns, default_name)

    # Call UDF for each specified column.
    for col in columns:
        df = df.withColumn(columns[col], last_udf(F.col(col)))

    return df

