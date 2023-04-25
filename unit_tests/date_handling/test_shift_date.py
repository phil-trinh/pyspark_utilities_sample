"""
Test the shift_date function in date_handling/date_changes.py.

shift_date(
    df: DataFrame,
    start_date_col: str,
    time_quantity: int,
    time_unit: str,
    output_col_name: str
) -> DataFrame:
"""

from pyspark.sql import types as T, DataFrame
import datetime

from ..unit_test_utils import answer_check

from transformations_library.date_handling import date_changes


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("start_date_str", T.StringType(), True),
        T.StructField("start_date_date", T.DateType(), True),
        T.StructField("start_date_datetime", T.TimestampType(), True),
    ])

    DATA = [
        ["a", "2022-01-11 21:55:09", datetime.date(2022, 1, 11), datetime.datetime(2022, 1, 11, 21, 55, 9)],
        ["b", "2021-11-23 15:04:02", datetime.date(2021, 11, 23), datetime.datetime(2021, 11, 23, 15, 4, 2)],
        ["c", "2022-06-18 11:06:12", datetime.date(2022, 6, 18), datetime.datetime(2022, 6, 18, 11, 6, 12)],
        ["d", None, None, None],
        ["e", "2022-02-06 09:08:06", datetime.date(2022, 2, 6), datetime.datetime(2022, 2, 6, 9, 8, 6)],
        ["f", "2021-10-14 04:12:26", datetime.date(2021, 10, 14), datetime.datetime(2021, 10, 14, 4, 12, 26)],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_shift_date_STR_0(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_str",
            time_quantity=4,
            time_unit="days",
            output_col_name="start_date_str_plus_4_days"
        )
    )

    answer = {
        "a": datetime.date(2022, 1, 15),
        "b": datetime.date(2021, 11, 27),
        "c": datetime.date(2022, 6, 22),
        "d": None,
        "e": datetime.date(2022, 2, 10),
        "f": datetime.date(2021, 10, 18),
    }

    answer_check(df, answer, "start_date_str_plus_4_days")


def test_shift_date_DATE_0(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_date",
            time_quantity=4,
            time_unit="days",
            output_col_name="start_date_date_plus_4_days"
            )
    )

    answer = {
        "a": datetime.date(2022, 1, 15),
        "b": datetime.date(2021, 11, 27),
        "c": datetime.date(2022, 6, 22),
        "d": None,
        "e": datetime.date(2022, 2, 10),
        "f": datetime.date(2021, 10, 18),
    }

    answer_check(df, answer, "start_date_date_plus_4_days")


def test_shift_date_TIMESTAMP_0(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_datetime",
            time_quantity=4,
            time_unit="days",
            output_col_name="start_date_datetime_plus_4_days"
        )
    )

    answer = {
        "a": datetime.date(2022, 1, 15),
        "b": datetime.date(2021, 11, 27),
        "c": datetime.date(2022, 6, 22),
        "d": None,
        "e": datetime.date(2022, 2, 10),
        "f": datetime.date(2021, 10, 18),
    }

    answer_check(df, answer, "start_date_datetime_plus_4_days")


def test_shift_date_STR_1(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_str",
            time_quantity=6,
            time_unit="weekdays",
            output_col_name="start_date_str_plus_6_weekdays"
        )
    )

    answer = {
        "a": datetime.date(2022, 1, 19),
        "b": datetime.date(2021, 12, 1),
        "c": datetime.date(2022, 6, 28),
        "d": None,
        "e": datetime.date(2022, 2, 15),
        "f": datetime.date(2021, 10, 22),
    }

    answer_check(df, answer, "start_date_str_plus_6_weekdays")


def test_shift_date_DATE_1(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_date",
            time_quantity=6,
            time_unit="weekdays",
            output_col_name="start_date_date_plus_6_weekdays"
        )
    )

    answer = {
        "a": datetime.date(2022, 1, 19),
        "b": datetime.date(2021, 12, 1),
        "c": datetime.date(2022, 6, 28),
        "d": None,
        "e": datetime.date(2022, 2, 15),
        "f": datetime.date(2021, 10, 22),
    }

    answer_check(df, answer, "start_date_date_plus_6_weekdays")


def test_shift_date_TIMESTAMP_1(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_datetime",
            time_quantity=6,
            time_unit="weekdays",
            output_col_name="start_date_datetime_plus_6_weekdays"
        )
    )

    answer = {
        "a": datetime.date(2022, 1, 19),
        "b": datetime.date(2021, 12, 1),
        "c": datetime.date(2022, 6, 28),
        "d": None,
        "e": datetime.date(2022, 2, 15),
        "f": datetime.date(2021, 10, 22),
    }

    answer_check(df, answer, "start_date_datetime_plus_6_weekdays")


def test_shift_date_STR_2(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_str",
            time_quantity=-2,
            time_unit="weeks",
            output_col_name="start_date_str_minus_2_weeks"
        )
    )

    answer = {
        "a": datetime.date(2021, 12, 28),
        "b": datetime.date(2021, 11, 9),
        "c": datetime.date(2022, 6, 4),
        "d": None,
        "e": datetime.date(2022, 1, 23),
        "f": datetime.date(2021, 9, 30),
    }

    answer_check(df, answer, "start_date_str_minus_2_weeks")


def test_shift_date_DATE_2(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_date",
            time_quantity=-2,
            time_unit="weeks",
            output_col_name="start_date_date_minus_2_weeks"
        )
    )

    answer = {
        "a": datetime.date(2021, 12, 28),
        "b": datetime.date(2021, 11, 9),
        "c": datetime.date(2022, 6, 4),
        "d": None,
        "e": datetime.date(2022, 1, 23),
        "f": datetime.date(2021, 9, 30),
    }

    answer_check(df, answer, "start_date_date_minus_2_weeks")


def test_shift_date_TIMESTAMP_2(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_datetime",
            time_quantity=-2,
            time_unit="weeks",
            output_col_name="start_date_datetime_minus_2_weeks"
        )
    )

    answer = {
        "a": datetime.date(2021, 12, 28),
        "b": datetime.date(2021, 11, 9),
        "c": datetime.date(2022, 6, 4),
        "d": None,
        "e": datetime.date(2022, 1, 23),
        "f": datetime.date(2021, 9, 30),
    }

    answer_check(df, answer, "start_date_datetime_minus_2_weeks")


def test_shift_date_STR_3(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_str",
            time_quantity=-1,
            time_unit="months",
            output_col_name="start_date_str_minus_1_months"
        )
    )

    answer = {
        "a": datetime.date(2021, 12, 11),
        "b": datetime.date(2021, 10, 23),
        "c": datetime.date(2022, 5, 18),
        "d": None,
        "e": datetime.date(2022, 1, 6),
        "f": datetime.date(2021, 9, 14),
    }

    answer_check(df, answer, "start_date_str_minus_1_months")


def test_shift_date_DATE_3(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_date",
            time_quantity=-1,
            time_unit="months",
            output_col_name="start_date_date_minus_1_months"
        )
    )

    answer = {
        "a": datetime.date(2021, 12, 11),
        "b": datetime.date(2021, 10, 23),
        "c": datetime.date(2022, 5, 18),
        "d": None,
        "e": datetime.date(2022, 1, 6),
        "f": datetime.date(2021, 9, 14),
    }

    answer_check(df, answer, "start_date_date_minus_1_months")


def test_shift_date_TIMESTAMP_3(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_datetime",
            time_quantity=-1,
            time_unit="months",
            output_col_name="start_date_datetime_minus_1_months"
        )
    )

    answer = {
        "a": datetime.date(2021, 12, 11),
        "b": datetime.date(2021, 10, 23),
        "c": datetime.date(2022, 5, 18),
        "d": None,
        "e": datetime.date(2022, 1, 6),
        "f": datetime.date(2021, 9, 14),
    }

    answer_check(df, answer, "start_date_datetime_minus_1_months")


def test_shift_date_STR_4(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_str",
            time_quantity=-1,
            time_unit="years",
            output_col_name="start_date_str_minus_1_years"
        )
    )

    answer = {
        "a": datetime.date(2021, 1, 11),
        "b": datetime.date(2020, 11, 23),
        "c": datetime.date(2021, 6, 18),
        "d": None,
        "e": datetime.date(2021, 2, 6),
        "f": datetime.date(2020, 10, 14),
    }

    answer_check(df, answer, "start_date_str_minus_1_years")


def test_shift_date_DATE_4(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_date",
            time_quantity=-1,
            time_unit="years",
            output_col_name="start_date_date_minus_1_years"
        )
    )

    answer = {
        "a": datetime.date(2021, 1, 11),
        "b": datetime.date(2020, 11, 23),
        "c": datetime.date(2021, 6, 18),
        "d": None,
        "e": datetime.date(2021, 2, 6),
        "f": datetime.date(2020, 10, 14),
    }

    answer_check(df, answer, "start_date_date_minus_1_years")


def test_shift_date_TIMESTAMP_4(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.shift_date(
            df,
            start_date_col="start_date_datetime",
            time_quantity=-1,
            time_unit="years",
            output_col_name="start_date_datetime_minus_1_years"
        )
    )

    answer = {
        "a": datetime.date(2021, 1, 11),
        "b": datetime.date(2020, 11, 23),
        "c": datetime.date(2021, 6, 18),
        "d": None,
        "e": datetime.date(2021, 2, 6),
        "f": datetime.date(2020, 10, 14),
    }

    answer_check(df, answer, "start_date_datetime_minus_1_years")

