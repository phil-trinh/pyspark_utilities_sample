"""
Table of Contents

1. cal_to_fiscal_year: Convert calendar year and month to fiscal year.
2. cal_to_fiscal_month: Convert calendar month to fiscal month.
3. fiscal_month_name: Add fiscal month's name from month number.
"""

from pyspark.sql import functions as F, types as T, DataFrame


def cal_to_fiscal_year(
    df: DataFrame,
    year_col: str,
    month_col: str,
    col_name: str = "fiscal_year",
    str_type: bool = True
) -> DataFrame:
    """
    Convert calendar year and month to fiscal year.

    Inputs
        df: DataFrame.
        year_col: Name of column containing calendar year as an integer or string.
        month_col: Name of column containing calendar month as an integer or string.
        col_name: Name of new column to add for fiscal year.
        str_type: True to return as a StringType; False to return as an IntegerType.

    Output
        Updated DataFrame with new fiscal year column.

    Example
        df = cal_to_fiscal_year(df, "year", "month", col_name="fiscal_year", str_type=False)
        Will add the column col_name to DataFrame df as an IntegerType.
    """
    def _fy_calc(year: [int, str], month: [int, str]) -> int:
        """
        Private function to convert calendar year and month to fiscal year.

        Inputs
            year: Calendar year as integer or string.
            month: Calendar month as integer or string.

        Output
            Fiscal year as integer.

        Example
            fy = _fy_calc(2021, 12)
            Will return 2022.
        """
        # Convert inputs to integer if necessary.
        if not isinstance(year, int):
            year = int(year)
        if not isinstance(month, int):
            month = int(month)

        # Logic check: if month is 10, 11, or 12, increment year to be the next FY.
        if month > 9:
            year += 1

        return year

    # Convert private function to a UDF and use to add new column.
    _fy_calc_udf = F.udf(_fy_calc, T.IntegerType())

    # Determine requested type for output column.
    output_type = "string" if str_type else "integer"

    return df.withColumn(col_name, _fy_calc_udf(F.col(year_col), F.col(month_col)).cast(output_type))


def cal_to_fiscal_month(
    df: DataFrame,
    month_col: str,
    col_name: str = "fiscal_month",
    str_type: bool = True,
    zero_pad: int = 3
) -> DataFrame:
    """
    Convert calendar month to fiscal month.

    Inputs
        df: DataFrame.
        month_col: Name of column containing calendar month as an integer or string.
        col_name: Name of new column to add for fiscal month.
        str_type: True to return as a StringType; False to return as an IntegerType.
        zero_pad: If a string is to be returned, ensure the string is padded.

    Output
        Updated DataFrame with new fiscal month column.

    Example
        df = cal_to_fiscal_month(df, "month", col_name="fiscal_month", str_type=True, zero_pad=3)
        Will add the column col_name to DataFrame df as a StringType padded to be length 3.
    """
    def _fm_calc(month: [int, str]) -> int:
        """
        Private function to convert calendar month to fiscal month.

        Inputs
            month: Calendar month as integer or string.
        Output
            Fiscal month as integer.
        Example
            fy = _fy_calc(12)
            Will return 3.
        """
        # Convert inputs to integer if necessary.
        if not isinstance(month, int):
            month = int(month)

        # Logic check: if month is 10, 11, or 12, subtract 9, otherwise add 3.
        if month > 9:
            month -= 9
        else:
            month += 3

        return month

    # Convert private function to a UDF and use to add new column.
    _fm_calc_udf = F.udf(_fm_calc, T.IntegerType())
    df = df.withColumn(col_name, _fm_calc_udf(F.col(month_col)))

    # Pad with zeros if a string has been requested.
    if str_type:
        df = df.withColumn(col_name, F.lpad(F.col(col_name).cast("string"), zero_pad, "0"))

    return df


def fiscal_month_name(
    df: DataFrame,
    month: str,
    col_name: str = "fiscal_month_name",
    full_name: bool = True,
    upper_case: bool = True
) -> DataFrame:
    """
    Add fiscal month's name from month number.

    Inputs
        df: DataFrame.
        month: Name of column with month number.
        col_name: Name of new column to add.
        full_name: True to return full month; False for the first three characters.
        upper_case: True to return name in all caps; False to return in Case form.

    Output
        Updated DataFrame with month's name column added.

    Example
        df = fiscal_month_name(df, month="month_number", col_name="month_name", full_name=False, upper_case=True)
        Will add the new column "month_name" containing the name of the month indexed by "month_number" in
        short (three letters) name, all caps form.
    """
    def _month_name(month_num: [int, str]) -> str:
        """
        Convert month's number to its name.

        Input
            month_num: Month's index number (1 - 12).

        Output
            Month name in string form.

        Example
            name = _month_name(4)
            Will return January.
                Note: Will pick up the variables full_name and upper_case from the scope of the outer function.
        """
        names_list = [
            "October", "November", "December",
            "January", "February", "March",
            "April", "May", "June",
            "July", "August", "September",
        ]

        # Shorten to first three characters if outer-scope variable indicates.
        if not full_name:
            names_list = [name[:3] for name in names_list]
        if upper_case:
            names_list = [name.upper() for name in names_list]

        # Convert to dictionary for easy lookup.
        names_dict = {(i + 1): name for (i, name) in enumerate(names_list)}

        # Ensure month_num is a proper index type.
        if not isinstance(month_num, int):
            month_num = int(month_num)

        return names_dict.get(month_num, None)

    _month_name_udf = F.udf(_month_name, T.StringType())

    df = df.withColumn(col_name, _month_name_udf(F.col(month)))

    return df
