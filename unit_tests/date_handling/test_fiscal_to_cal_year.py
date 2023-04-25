"""
Test the fiscal_to_cal_year function in date_handling/to_calendar.py.

fiscal_to_cal_year(
    df: DataFrame,
    year_col: str,
    month_col: str,
    is_fiscal_month: bool = True,
    col_name: str = "calendar_year",
    str_type: bool = True
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.date_handling import to_calendar


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
        T.StructField("year", T.StringType(), True),
        T.StructField("month", T.StringType(), True),
    ])

    DATA = [
        ["a", "2022", "001"],
        ["b", "2022", "003"],
        ["c", "2022", "4"],
        ["d", None, None],
        ["e", "2022", "12"]
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_fiscal_to_cal_year_0(spark_session) -> None:
    """
    Test the function: return as a string.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.fiscal_to_cal_year(df, "year", "month")
    answer = {
        "a": "2021",  # 2022 means FY22, 001 means October = Oct 2021
        "b": "2021",  # 2022 means FY22, 003 means December = Dec 2021
        "c": "2022",  # 2022 means FY22, 4 means January = Jan 2022
        "d": None,
        "e": "2022",  # 2022 means FY22, 12 means September = Sep 2022
    }
    answer_check(df, answer, "calendar_year")


def test_fiscal_to_cal_year_1(spark_session) -> None:
    """
    Test the function: return as an integer.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.fiscal_to_cal_year(df, "year", "month", str_type=False)
    answer = {
        "a": 2021,
        "b": 2021,
        "c": 2022,
        "d": None,
        "e": 2022,
    }
    answer_check(df, answer, "calendar_year")


def test_fiscal_to_cal_year_2(spark_session) -> None:
    """
    Test the function: return as an integer, treat the months as calendar months not fiscal months.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_calendar.fiscal_to_cal_year(df, "year", "month", is_fiscal_month=False, str_type=False)
    answer = {
        "a": 2022,  # 2022 means FY22, 001 means January = Jan 2022
        "b": 2022,  # 2022 means FY22, 003 means March = Mar 2022
        "c": 2022,  # 2022 means FY22, 4 means April = Apr 2022
        "d": None,
        "e": 2021,  # 2022 means FY22, 12 means December = Dec 2021 (to be in FY22, must be year earlier)
    }
    answer_check(df, answer, "calendar_year")

