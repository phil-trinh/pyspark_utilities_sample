"""
Test the cal_to_fiscal_month function in date_handling/to_fiscal.py.

def cal_to_fiscal_month(
    df: DataFrame,
    month_col: str,
    col_name: str = "fiscal_month",
    str_type: bool = True,
    zero_pad: int = 3
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
        T.StructField("month", T.StringType(), True),
    ])

    DATA = [
        ["a", "001"],
        ["b", "003"],
        ["c", "010"],
        ["d", None],
        ["e", "009"],
        # Notice: There are no upper and lower bound limits to the input for months.
        ["f", "013"],  # Documenting what happens at month "13".
        ["g", "000"],  # Documenting what happens at month "0".
        ["h", "024"],  # Documenting what happens at month "24".
        ["i", "-004"],  # Documenting what happens at month "-4".
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_cal_to_fiscal_month_0(spark_session) -> None:
    """
    Test the function: return as a string. Zero pad = default of 3

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.cal_to_fiscal_month(df, "month")

    answer = {
        "a": "004",
        "b": "006",
        "c": "001",
        "d": None,
        "e": "012",
        "f": "004",
        "g": "003",
        "h": "015",
        "i": "0-1",
    }

    answer_check(df, answer, "fiscal_month")


def test_cal_to_fiscal_month_1(spark_session) -> None:
    """
    Test the function: return as an integer.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.cal_to_fiscal_month(df, "month", str_type=False)

    answer = {
        "a": 4,
        "b": 6,
        "c": 1,
        "d": None,
        "e": 12,
        "f": 4,
        "g": 3,
        "h": 15,
        "i": -1,
    }

    answer_check(df, answer, "fiscal_month")


def test_cal_to_fiscal_month_2(spark_session) -> None:
    """
    Test the function: return as string with Zero pad = 2

    Input
        spark_sesssion: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = to_fiscal.cal_to_fiscal_month(df, "month", zero_pad=2)

    answer = {
        "a": "04",
        "b": "06",
        "c": "01",
        "d": None,
        "e": "12",
        "f": "04",
        "g": "03",
        "h": "15",
        "i": "-1",
    }

    answer_check(df, answer, "fiscal_month")

