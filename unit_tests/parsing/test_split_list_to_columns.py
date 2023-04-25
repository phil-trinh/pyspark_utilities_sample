"""
Test the split_list_to_columns function in parsing/array_fun.py.

split_list_to_columns(
    data: DataFrame,
    col_index: str,
    col_target: str,
    dtype: Callable = str,
    dtype_target: Callable = T.StringType(),
    drop_col_target: bool = False
) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.array_fun import split_list_to_columns


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the melt function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("col_str_list", T.ArrayType(T.StringType(), True), True),
        T.StructField("col_int_list", T.ArrayType(T.IntegerType(), True), True),
    ])

    DATA = [
        ["a", ["a1", "a2", "a3"], [1, 2, 3]],
        ["b", ["b4", "b5", "b6"], [4, 5, 6]],
        ["c", [None], []],
        ["d", ["d7", None], [7, None]],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_split_list_to_columns_str_0(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of StringType values.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = split_list_to_columns(df, col_index="key", col_target="col_str_list", dtype_target=T.StringType())
    answer = {
        "a": "a1",
        "b": "b4",
        "c": None,
        "d": "d7",
    }

    answer_check(df, answer, "col_str_list_0", key_col="key")


def test_split_list_to_columns_str_1(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of StringType values.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = split_list_to_columns(df, col_index="key", col_target="col_str_list", dtype_target=T.StringType())
    answer = {
        "a": "a2",
        "b": "b5",
        "c": None,
        "d": None,
    }

    answer_check(df, answer, "col_str_list_1", key_col="key")


def test_split_list_to_columns_str_2(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of StringType values.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = split_list_to_columns(df, col_index="key", col_target="col_str_list", dtype_target=T.StringType())
    answer = {
        "a": "a3",
        "b": "b6",
        "c": None,
        "d": None,
    }

    answer_check(df, answer, "col_str_list_2", key_col="key")


def test_split_list_to_columns_int_0(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of IntegerType values.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        split_list_to_columns(
            df,
            col_index="key",
            col_target="col_int_list",
            dtype=int,
            dtype_target=T.IntegerType()
        )
    )
    answer = {
        "a": 1,
        "b": 4,
        "c": None,
        "d": 7,
    }

    answer_check(df, answer, "col_int_list_0", key_col="key")


def test_split_list_to_columns_int_1(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of IntegerType values.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        split_list_to_columns(
            df,
            col_index="key",
            col_target="col_int_list",
            dtype=int,
            dtype_target=T.IntegerType()
        )
    )
    answer = {
        "a": 2,
        "b": 5,
        "c": None,
        "d": None,
    }

    answer_check(df, answer, "col_int_list_1", key_col="key")


def test_split_list_to_columns_int_2(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of IntegerType values.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        split_list_to_columns(
            df,
            col_index="key",
            col_target="col_int_list",
            dtype=int,
            dtype_target=T.IntegerType()
        )
    )
    answer = {
        "a": 3,
        "b": 6,
        "c": None,
        "d": None,
    }

    answer_check(df, answer, "col_int_list_2", key_col="key")


def test_split_list_to_columns_str_drop_0(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of StringType values,
    and ensure the original column is dropped.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        split_list_to_columns(
            df,
            col_index="key",
            col_target="col_str_list",
            dtype_target=T.StringType(),
            drop_col_target=True
            )
    )

    assert ("col_str_list" not in df.columns)


def test_split_list_to_columns_str_keep_0(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of StringType values,
    and ensure the original column is kept.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        split_list_to_columns(
            df,
            col_index="key",
            col_target="col_str_list",
            dtype_target=T.StringType(),
            drop_col_target=False
            )
    )

    assert ("col_str_list" in df.columns)


def test_split_list_to_columns_int_drop_0(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of IntegerType values,
    and ensure the original column is dropped.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        split_list_to_columns(
            df,
            col_index="key",
            col_target="col_int_list",
            dtype=int,
            dtype_target=T.IntegerType(),
            drop_col_target=True
        )
    )

    assert ("col_int_list" not in df.columns)


def test_split_list_to_columns_int_keep_0(spark_session) -> None:
    """
    Test the function split_list_to_columns on lists of IntegerType values,
    and ensure the original column is kept.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = (
        split_list_to_columns(
            df,
            col_index="key",
            col_target="col_int_list",
            dtype=int,
            dtype_target=T.IntegerType(),
            drop_col_target=False
        )
    )

    assert ("col_int_list" in df.columns)

