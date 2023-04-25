"""
Test the time_between_dates function in date_handling/date_changes.py.

time_between_dates(
    df: DataFrame,
    start_date_col: str,
    end_date_col: str,
    time_unit: str,
    output_col_name: str,
    rounding: int = 2
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
        T.StructField("end_date_str", T.StringType(), True),
        T.StructField("start_date_date", T.DateType(), True),
        T.StructField("end_date_date", T.DateType(), True),
        T.StructField("start_date_datetime", T.TimestampType(), True),
        T.StructField("end_date_datetime", T.TimestampType(), True),
    ])

    DATA = [
        [
            "a", "2022-01-11 15:00:00", "2022-01-20 13:00:00",
            datetime.date(2022, 1, 11), datetime.date(2022, 1, 20),
            datetime.datetime(2022, 1, 11, 15, 0, 0), datetime.datetime(2022, 1, 20, 13, 0, 0),
        ],
        ["b", None, None, None, None, None, None],
        [
            "c", "2021-07-14 02:11:20", "2022-03-13 05:02:12",
            datetime.date(2021, 7, 14), datetime.date(2022, 3, 13),
            datetime.datetime(2021, 7, 14, 2, 11, 20), datetime.datetime(2022, 3, 13, 5, 2, 12),
        ],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_time_between_dates_STR_0(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_str",
            end_date_col="end_date_str",
            time_unit="days",
            output_col_name="days_from_start_date_str_to_end_date_str"
        )
    )

    answer = {
        "a": 9,
        "b": None,
        "c": 242,
    }

    answer_check(df, answer, "days_from_start_date_str_to_end_date_str")


def test_time_between_dates_DATE_0(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_date",
            end_date_col="end_date_date",
            time_unit="days",
            output_col_name="days_from_start_date_date_to_end_date_date"
        )
    )

    answer = {
        "a": 9,
        "b": None,
        "c": 242,
    }

    answer_check(df, answer, "days_from_start_date_date_to_end_date_date")


def test_time_between_dates_TIMESTAMP_0(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_datetime",
            end_date_col="end_date_datetime",
            time_unit="days",
            output_col_name="days_from_start_date_datetime_to_end_date_datetime"
        )
    )

    answer = {
        "a": 9,
        "b": None,
        "c": 242,
    }

    answer_check(df, answer, "days_from_start_date_datetime_to_end_date_datetime")


def test_time_between_dates_STR_1(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_str",
            end_date_col="end_date_str",
            time_unit="weekdays",
            output_col_name="weekdays_from_start_date_str_to_end_date_str"
        )
    )

    answer = {
        "a": 7,
        "b": None,
        "c": 172,
    }

    answer_check(df, answer, "weekdays_from_start_date_str_to_end_date_str")


def test_time_between_dates_DATE_1(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_date",
            end_date_col="end_date_date",
            time_unit="weekdays",
            output_col_name="weekdays_from_start_date_date_to_end_date_date"
        )
    )

    answer = {
        "a": 7,
        "b": None,
        "c": 172,
    }

    answer_check(df, answer, "weekdays_from_start_date_date_to_end_date_date")


def test_time_between_dates_TIMESTAMP_1(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_datetime",
            end_date_col="end_date_datetime",
            time_unit="weekdays",
            output_col_name="weekdays_from_start_date_datetime_to_end_date_datetime"
        )
    )

    answer = {
        "a": 7,
        "b": None,
        "c": 172,
    }

    answer_check(df, answer, "weekdays_from_start_date_datetime_to_end_date_datetime")


def test_time_between_dates_STR_2(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_str",
            end_date_col="end_date_str",
            time_unit="weeks",
            output_col_name="weeks_from_start_date_str_to_end_date_str"
        )
    )

    answer = {
        "a": 1.29,
        "b": None,
        "c": 34.57,
    }

    answer_check(df, answer, "weeks_from_start_date_str_to_end_date_str")


def test_time_between_dates_DATE_2(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_date",
            end_date_col="end_date_date",
            time_unit="weeks",
            output_col_name="weeks_from_start_date_date_to_end_date_date"
        )
    )

    answer = {
        "a": 1.29,
        "b": None,
        "c": 34.57,
    }

    answer_check(df, answer, "weeks_from_start_date_date_to_end_date_date")


def test_time_between_dates_TIMESTAMP_2(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_datetime",
            end_date_col="end_date_datetime",
            time_unit="weeks",
            output_col_name="weeks_from_start_date_datetime_to_end_date_datetime"
        )
    )

    answer = {
        "a": 1.29,
        "b": None,
        "c": 34.57,
    }

    answer_check(df, answer, "weeks_from_start_date_datetime_to_end_date_datetime")


def test_time_between_dates_STR_3(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_str",
            end_date_col="end_date_str",
            time_unit="months",
            output_col_name="months_from_start_date_str_to_end_date_str"
        )
    )

    answer = {
        "a": 0.29,
        "b": None,
        "c": 7.97,
    }

    answer_check(df, answer, "months_from_start_date_str_to_end_date_str")


def test_time_between_dates_DATE_3(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_date",
            end_date_col="end_date_date",
            time_unit="months",
            output_col_name="months_from_start_date_date_to_end_date_date"
        )
    )

    answer = {
        "a": 0.29,
        "b": None,
        "c": 7.97,
    }

    answer_check(df, answer, "months_from_start_date_date_to_end_date_date")


def test_time_between_dates_TIMESTAMP_3(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_datetime",
            end_date_col="end_date_datetime",
            time_unit="months",
            output_col_name="months_from_start_date_datetime_to_end_date_datetime"
        )
    )

    answer = {
        "a": 0.29,
        "b": None,
        "c": 7.97,
    }

    answer_check(df, answer, "months_from_start_date_datetime_to_end_date_datetime")


def test_time_between_dates_STR_4(spark_session) -> None:
    """
    Test the function: input as a str, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_str",
            end_date_col="end_date_str",
            time_unit="years",
            output_col_name="years_from_start_date_str_to_end_date_str"
        )
    )

    answer = {
        "a": 0.02,
        "b": None,
        "c": 0.66,
    }

    answer_check(df, answer, "years_from_start_date_str_to_end_date_str")


def test_time_between_dates_DATE_4(spark_session) -> None:
    """
    Test the function: input as a date, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_date",
            end_date_col="end_date_date",
            time_unit="years",
            output_col_name="years_from_start_date_date_to_end_date_date"
        )
    )

    answer = {
        "a": 0.02,
        "b": None,
        "c": 0.66,
    }

    answer_check(df, answer, "years_from_start_date_date_to_end_date_date")


def test_time_between_dates_TIMESTAMP_4(spark_session) -> None:
    """
    Test the function: input as a timestamp, return as a date.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        date_changes.time_between_dates(
            df,
            start_date_col="start_date_datetime",
            end_date_col="end_date_datetime",
            time_unit="years",
            output_col_name="years_from_start_date_datetime_to_end_date_datetime"
        )
    )

    answer = {
        "a": 0.02,
        "b": None,
        "c": 0.66,
    }

    answer_check(df, answer, "years_from_start_date_datetime_to_end_date_datetime")

