"""
Table of Contents

1. shift_date: Add or subtract time units (days, weekdays, weeks, months, or years) to/from a given date.
2. time_between_dates: Calculate time (in days, weekdays, weeks, months, or years) between a given start
                       and end date.
"""

from datetime import timedelta
from numpy import busday_offset, datetime64
from pandas import to_datetime  # noqa
from pyspark.sql import functions as F, types as T, DataFrame


def shift_date(
    df: DataFrame,
    start_date_col: str,
    time_quantity: int,
    time_unit: str,
    output_col_name: str = "output_from_shift_date"
) -> DataFrame:
    """
    Add or subtract time units (days, weekdays, weeks, months, or years) to/from a given date.

    Inputs
        df: DataFrame.
        start_date_col: Date you will add/subtract time units to/from.
                        Date format can be timestamp, date, or
                        date-formatted string (e.g., "2021-01-01" or "2021-01-01 11:25:24")
        time_quantity: Integer number capturing the amount of time you want to add to or subtract from start_date_col.
        time_unit: The unit of time; must be one of five options: ["days","weekdays","weeks","months","years"]
                        Notes on "weekdays" time_unit:
                            PERFORMANCE: The "weekdays" argument might be slighly slower than the others, as it
                                incorporates external libraries (numpy and pandas) on top of PySpark.
                                Keep that in mind if performance is critical.
                            WEEKENDS: Weekends are not counted. If a value in start_date_col falls on a weekend,
                                then the following Monday is treated as the start date for calculation purposes.
                            HOLIDAYS: Holidays falling on weekdays are currently counted as weekdays.
        output_col_name: The name of the output column.
                         If left to the default, output_col_name will reflect the name of the start_date_col.

    Output
        Updated DataFrame with column(s) added.

    Example
        df = shift_date(df, start_date_col="date_column", time_quantity=4, time_unit="months")
        Will return an updated DataFrame with a new column, "date_column_plus_4_months".
    """

    # if output_col_name is set to default, rename it based on name of start_date_col (input column).
    if output_col_name == "output_from_shift_date":
        # Determine if time_quantity adds or subtracts.
        if time_quantity >= 0:
            plus_or_minus = "plus"
        else:
            plus_or_minus = "minus"
        output_col_name = start_date_col + "_" + plus_or_minus + "_" + str(abs(time_quantity)) + "_" + str(time_unit)

    # Cast results in date type format, aligning with PySpark date functionality.
    out_type = T.DateType()

    @F.udf(T.TimestampType())
    def add_weekdays(start_date: str) -> T.TimestampType:
        """
        Add weekdays to a given date. This function is used in the shift_date function.

        Inputs
            start_date: Date you will add time units to.

        Output
            Date value.
        """
        if start_date is not None:
            return to_datetime(
                busday_offset(
                    datetime64(start_date, 'D'),
                    time_quantity,
                    roll="forward"
                    )
            )
        else:
            return None

    # Based on time_unit argument, calculate output column and add it to dataframe.
    if time_unit == "days":
        return df.withColumn(output_col_name, F.date_add(F.col(start_date_col), time_quantity).cast(out_type))
    elif time_unit == "weekdays":
        return df.withColumn(output_col_name, add_weekdays(F.col(start_date_col)).cast(out_type))
    elif time_unit == "weeks":
        return df.withColumn(output_col_name, F.date_add(F.col(start_date_col), 7*time_quantity).cast(out_type))
    elif time_unit == "months":
        return df.withColumn(output_col_name, F.add_months(F.col(start_date_col), time_quantity).cast(out_type))
    elif time_unit == "years":
        return df.withColumn(output_col_name, F.add_months(F.col(start_date_col), 12*time_quantity).cast(out_type))
    # if none of the time_unit arguments above are found, raise an error
    else:
        raise ValueError("Wrong time unit; choose from ['days','weekdays','weeks','months','years']")


def time_between_dates(
    df: DataFrame,
    start_date_col: str,
    end_date_col: str,
    time_unit: str,
    output_col_name: str,
    rounding: int = 2
) -> DataFrame:
    """
    Calculate timedelta (in days, weekdays, weeks, months, or years) between a given start and end date.

    In the returned dataframe, the new output column is rounded to the second decimal place for
    all time_unit options except for "days", which returns whole numbers.

    Inputs
        df: DataFrame.
        start_date_col: Start date. Date format can be timestamp, date, or
                        date-formatted string (e.g., "2021-01-01" or "2021-01-01 11:25:24")
        end_date_col: End date. Date format can be timestamp, date, or
                        date-formatted string (e.g., "2021-01-01" or "2021-01-01 11:25:24")
        time_unit: The unit of time; must be one of five options: ["days","weekdays","weeks","months","years"]
                        Notes on "weekdays" time_unit:
                            PERFORMANCE: The "weekdays" argument might be slighly slower than the others, as it
                                incorporates an external library (datetime) on top of PySpark via a UDF.
                                Keep that in mind if performance is critical.
                            WEEKENDS: Weekends are not counted. If a value in start_date_col falls on a weekend,
                                then the following Monday is treated as the start date for calculation purposes.
                                If a value in end_date_col falls on a weekend, then the preceding Friday is
                                treated as the end date for calculation purposes.
                            HOLIDAYS: Holidays falling on weekdays are currently counted as weekdays.
        output_col_name: The name of the output column.
        rounding: Number of decimal places in output column.

    Output
        Updated DataFrame containing a new column capturing the timedelta between start and end dates.

    Example
        df = time_between_dates(
            df,
            start_date_col="first_date",
            end_date_col="last_date",
            time_unit="weekdays",
            output_col_name="weekdays_between_dates"
        )
        Will return an updated DataFrame with a new column, "weekdays_between_dates".
    """

    # Define weekdays_between_dates udf, used in if-elif-else statement below.
    @F.udf(T.IntegerType())
    def weekdays_between_dates(start_date, end_date) -> int:
        """
        Calculate weekdays between a given start date and end date.
        This function is used in the time_between_dates function.

        Inputs
            start_date: Start date.
            end_date: End date.

        Output
            Number of weekdays between a given start date and end date.
        """
        if (start_date is not None) and (end_date is not None):
            start_date, end_date = to_datetime(start_date).date(), to_datetime(end_date).date()
            # Generating dates.
            dates = (
                start_date + timedelta(idx + 1)
                for idx in range((end_date - start_date).days)
            )

            # Summing all weekdays.
            num_weekdays = sum(1 for day in dates if day.weekday() < 5)

            return num_weekdays
        else:
            # Return None if either start_date or end_date is blank.
            return None

    # Based on time_unit argument, calculate output column and add it to dataframe.
    if time_unit == "days":
        df = df.withColumn(output_col_name, F.datediff(F.col(end_date_col), F.col(start_date_col)))
    elif time_unit == "weekdays":
        df = df.withColumn(output_col_name, weekdays_between_dates(F.col(start_date_col), F.col(end_date_col)))
    elif time_unit == "weeks":
        df = df.withColumn(output_col_name, (1/7) * F.datediff(F.col(end_date_col), F.col(start_date_col)))
    elif time_unit == "months":
        df = df.withColumn(output_col_name, F.months_between(end_date_col, start_date_col))
    elif time_unit == "years":
        df = df.withColumn(output_col_name, (1/12) * F.months_between(end_date_col, start_date_col))
    else:
        # Raise error if time_unit is invalid.
        raise ValueError("Wrong time unit; choose from ['days','weekdays','weeks','months','years']")

    # Round before returning df.
    df = df.withColumn(output_col_name, F.round(F.col(output_col_name), rounding))

    return df

