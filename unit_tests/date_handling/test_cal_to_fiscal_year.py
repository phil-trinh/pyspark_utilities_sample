
"""
Test the cal_to_fiscal_year function in date_handling/to_calendar.py.

def cal_to_fiscal_year(
    df: DataFrame,
    year_col: str,
    month_col: str,
    col_name: str = "fiscal_year",
    str_type: bool = True,
    fy_add: bool = False
) -> DataFrame:
"""

from pyspark.sql import types as T, DataFrame
from ..unit_test_utils import answer_check

from transformations_library.date_handling import to_fiscal


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
        ["b", "2021", "012"],
        ["c", "2022", "010"],
        ["d", None, None],
        ["e", "2023", "009"],
        # Notice: There are no upper and lower bound limits to the input for months.
        ["f", "2021", "013"],  # Documenting what happens at month "13".
        ["g", "2022", "000"],  # Documenting what happens at month "0".
        ["h", "2023", "024"],  # Documenting what happens at month "24".
        ["i", "2019", "-004"],  # Documenting what happens at month "-4".
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_cal_to_fiscal_year_0(spark_session) -> None:
    """
    Test the function: return as a string.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.cal_to_fiscal_year(df, "year", "month")

    answer = {
        "a": "2022",  # 2022 means CY22, 001 means January = Jan 2022
        "b": "2022",  # 2021 means CY21, 012 means December = Dec 2022
        "c": "2023",  # 2022 means CY22, 010 means October = Oct 2022
        "d": None,
        "e": "2023",  # 2023 means CY23, 009 means September = Sep 2023
        "f": "2022",
        "g": "2022",
        "h": "2024",
        "i": "2019",
    }

    answer_check(df, answer, "fiscal_year")


def test_cal_to_fiscal_year_1(spark_session) -> None:
    """
    Test the function: return as an integer.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.cal_to_fiscal_year(df, "year", "month", str_type=False)

    answer = {
        "a": 2022,
        "b": 2022,
        "c": 2023,
        "d": None,
        "e": 2023,
        "f": 2022,
        "g": 2022,
        "h": 2024,
        "i": 2019,
    }

    answer_check(df, answer, "fiscal_year")


def test_cal_to_fiscal_year_2(spark_session) -> None:
    """
    Test the function: Fiscal Year shorthand notation.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.cal_to_fiscal_year(df, "year", "month", fy_shorthand=True)

    answer = {
        "a": "FY22",
        "b": "FY22",
        "c": "FY23",
        "d": None,
        "e": "FY23",
        "f": "FY22",
        "g": "FY22",
        "h": "FY24",
        "i": "FY19",
    }

    answer_check(df, answer, "fiscal_year")


def test_cal_to_fiscal_year_3(spark_session) -> None:
    """
    Test the function: Test fiscal year shorthand notation against integer input

    Input
        spark_session: Globally available vairable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.cal_to_fiscal_year(df, "year", "month", fy_shorthand=True, str_type=False)

    answer = {
        "a": "FY22",
        "b": "FY22",
        "c": "FY23",
        "d": None,
        "e": "FY23",
        "f": "FY22",
        "g": "FY22",
        "h": "FY24",
        "i": "FY19",
    }

    answer_check(df, answer, "fiscal_year")

