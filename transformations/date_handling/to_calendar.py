"""
Table of Contents

1. fiscal_to_cal_year: Convert fiscal year and month to calendar year.
2. fiscal_to_cal_month: Convert fiscal month to calendar month.
3. calendar_month_name: Add calendar month's name from month number.
4. date_col_from_fy: Create a date type column from an FY formatted column.
"""

from pyspark.sql import functions as F, types as T, DataFrame


def fiscal_to_cal_year(
    df: DataFrame,
    year_col: str,
    month_col: str,
    col_name: str = "calendar_year",
    str_type: bool = True
) -> DataFrame:
    """
    Convert fiscal year and month to calendar year.

    Inputs
        df: DataFrame.
        year_col: Name of column containing fiscal year as an integer or string.
        month_col: Name of column containing fiscal month as an integer or string.
        col_name: Name of new column to add for calendar year.
        str_type: True to return as a StringType; False to return as an IntegerType.

    Output
        Updated DataFrame with new calendar year column.

    Example
        df = fiscal_to_cal_year(df, "year", "month", col_name="calendar_year", str_type=False)
        Will add the column col_name to DataFrame df as an IntegerType.
    """
    def _cy_calc(year: [int, str], month: [int, str]) -> int:
        """
        Private function to convert fiscal year and month to calendar year.

        Inputs
            year: Fiscal year as integer or string.
            month: Fiscal month as integer or string.

        Output
            Calendar year as integer.

        Example
            cy = _cy_calc(2022, 3)
            Will return 2021.
        """
        # Convert inputs to integer if necessary.
        if not isinstance(year, int):
            year = int(year)
        if not isinstance(month, int):
            month = int(month)

        # Logic check: if month is 1, 2, or 3, decrement year to be the previous CY.
        if month < 4:
            year -= 1

        return year

    # Convert private function to a UDF and use to add new column.
    _cy_calc_udf = F.udf(_cy_calc, T.IntegerType())

    # Determine requested type for output column.
    output_type = "string" if str_type else "integer"

    return df.withColumn(col_name, _cy_calc_udf(F.col(year_col), F.col(month_col)).cast(output_type))


def fiscal_to_cal_month(
    df: DataFrame,
    month_col: str,
    col_name: str = "calendar_month",
    str_type: bool = False,
    zero_pad: int = 1
) -> DataFrame:
    """
    Convert fiscal month to calendar month.

    Inputs
        df: DataFrame.
        month_col: Name of column containing fiscal month as an integer or string.
        col_name: Name of new column to add for calendar month.
        str_type: True to return as a StringType; False to return as an IntegerType.
        zero_pad: If a string is to be returned, ensure the string is padded.

    Output
        Updated DataFrame with new fiscal month column.

    Example
        df = cal_to_fiscal_month(df, "month", col_name="fiscal_month", str_type=False)
        Will add the column col_name to DataFrame df as an IntegerType.
    """
    def _cm_calc(month: [int, str]) -> int:
        """
        Private function to convert fiscal month to calendar month.

        Inputs
            month: Fiscal month as integer or string.

        Output
            Calendar month as integer.

        Example
            cy = _cy_calc(12)
            Will return 9.
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
    _cm_calc_udf = F.udf(_cm_calc, T.IntegerType())
    df = df.withColumn(col_name, _cm_calc_udf(F.col(month_col)))

    # Pad with zeros if a string has been requested.
    if str_type:
        df = df.withColumn(col_name, F.lpad(F.col(col_name).cast("string"), zero_pad, "0"))

    return df


def calendar_month_name(
    df: DataFrame,
    month: str,
    col_name: str = "calendar_month_name",
    full_name: bool = True,
    upper_case: bool = True
) -> DataFrame:
    """
    Add calendar month's name from month number.

    Inputs
        df: DataFrame.
        month: Name of column with month number.
        col_name: Name of new column to add.
        full_name: True to return full month; False for the first three characters.
        upper_case: True to return name in all caps; False to return in Case form.

    Output
        Updated DataFrame with month's name column added.

    Example
        df = calendar_month_name(df, month="month_number", col_name="month_name", full_name=False, upper_case=True)
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
            Will return April.
                Note: Will pick up the variables full_name and upper_case from the scope of the outer function.
        """
        names_list = [
            "January", "February", "March",
            "April", "May", "June",
            "July", "August", "September",
            "October", "November", "December",
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


def date_col_from_fy(df: DataFrame, fy_col: str, date_col: str, quarterly: bool = False) -> DataFrame:
    """
    Create a date type column from an FY formatted column.

    Inputs
        df: Input dataframe.
        fy_col: Name of FY column to create date column.
        date_col: Name of the resulting date column.
        quarterly: True if there is a quarter designation in the FY_col.

    Output
        Resulting dataframe with date column appended.

    Example
        df = calendar_month_name(df, fy_col="FY21Q4", date_col="date", quarterly=True)
        Will add the new column "date" containing the last day of the calendar-converted quarter.
    """
    # Replace 'FY' with 20 (this would have to be changed at the turn of the century but we'll get to it then).
    df = df.withColumn(date_col, F.regexp_replace(F.col(fy_col), 'FY', '20'))
    tmp_df = df.withColumn('tmp_year', F.substring(F.col(date_col), 0, 4).cast("integer"))

    if not quarterly:
        # For columns that only have fiscal year, add in first quarter designation to set date later.
        tmp_df = tmp_df.withColumn('fiscal_quarter', F.lit('Q1'))
    else:
        tmp_df = tmp_df.withColumn('fiscal_quarter', F.substring(F.col(fy_col), 5, 2))

    tmp_df = (
        tmp_df
        .withColumn(
            'tmp_year',

            # If in first quarter of fiscal year, calendar date is 1 year previous.
            F.when(F.col('fiscal_quarter').contains('Q1'), F.col('tmp_year') - F.lit(1))
            .otherwise(F.col('tmp_year'))
        )
        .withColumn(
            date_col,

            # Set date for last day of each quarter.
            F.when(F.col('fiscal_quarter').contains('Q1'), F.concat(F.col('tmp_year'), F.lit('-12-31')))
             .when(F.col('fiscal_quarter').contains('Q2'), F.concat(F.col('tmp_year'), F.lit('-03-31')))
             .when(F.col('fiscal_quarter').contains('Q3'), F.concat(F.col('tmp_year'), F.lit('-06-30')))
             .when(F.col('fiscal_quarter').contains('Q4'), F.concat(F.col('tmp_year'), F.lit('-09-30')))
             .otherwise(None)
        )
    )

    # Cast to date type.
    tmp_df = tmp_df.withColumn(date_col, F.col(date_col).cast("date")).drop('tmp_year', 'fiscal_quarter')

    return tmp_df
