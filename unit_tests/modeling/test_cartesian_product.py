"""
Test the cartesian_product function in modeling/missing_values.py.

cartesian_product(df: DataFrame, columns: list = None, values: dict = None) -> DataFrame
"""

from pyspark.sql import types as T, DataFrame

from transformations_library.modeling.missing_values import cartesian_product


def create_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("col_a", T.StringType(), True),
        T.StructField("col_b", T.StringType(), True),
        T.StructField("col_c", T.IntegerType(), True),
    ])

    DATA = [
        [1, "a", "hello", 1],
        [2, "a", "world", 2],
        [3, "a", "sometimes", 1],
        [4, "b", "hello", 1],
        [5, "b", "world", 1],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def answer_df_0(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("col_a", T.StringType(), True),
        T.StructField("col_b", T.StringType(), True),
        T.StructField("col_c", T.IntegerType(), True),
    ])

    DATA = [
        [1, "a", "hello", 1],
        [2, "a", "world", 2],
        [3, "a", "sometimes", 1],
        [4, "b", "hello", 1],
        [5, "b", "world", 1],
        [None, "b", "sometimes", None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_cartesian_product_0(spark_session) -> None:
    """
    Test the function: simple style of two columns with all values present.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = cartesian_product(df, ["col_a", "col_b"])

    answer = answer_df_0(spark_session)

    assert df.schema == answer.schema, "Schema check for answer_df_0 failed."
    assert set(df.collect()) == set(answer.collect()), "Value check for answer_df_0 failed."  # noqa


def answer_df_1(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("col_a", T.StringType(), True),
        T.StructField("col_b", T.StringType(), True),
        T.StructField("col_c", T.IntegerType(), True),
    ])

    DATA = [
        [1, "a", "hello", 1],
        [2, "a", "world", 2],
        [3, "a", "sometimes", 1],
        [4, "b", "hello", 1],
        [5, "b", "world", 1],
        [None, "a", "hello", 2],
        [None, "a", "world", 1],
        [None, "a", "sometimes", 2],
        [None, "b", "hello", 2],
        [None, "b", "world", 2],
        [None, "b", "sometimes", 1],
        [None, "b", "sometimes", 2],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_cartesian_product_1(spark_session) -> None:
    """
    Test the function: expanded style of a column that has missing values.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = cartesian_product(df, ["col_a", "col_b"], {"col_c": [1, 2]})

    answer = answer_df_1(spark_session)

    assert df.schema == answer.schema, "Schema check for answer_df_1 failed."
    assert set(df.collect()) == set(answer.collect()), "Value check for answer_df_1 failed."  # noqa


def answer_df_2(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("id", T.IntegerType(), True),
        T.StructField("col_a", T.StringType(), True),
        T.StructField("col_b", T.StringType(), True),
        T.StructField("col_c", T.IntegerType(), True),
    ])

    DATA = [
        [1, "a", "hello", 1],
        [4, "b", "hello", 1],
        [None, None, "hello", 2],
        [2, "a", "world", 2],
        [5, "b", "world", 1],
        [3, "a", "sometimes", 1],
        [None, None, "goodbye", 1],
        [None, None, "goodbye", 2],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_cartesian_product_2(spark_session) -> None:
    """
    Test the function: expanded style of a column that has missing values, not including any "full" columns.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df = create_dataframe(spark_session)
    df = cartesian_product(df, None, {"col_b": ["hello", "world", "goodbye"], "col_c": [1, 2]})

    answer = answer_df_2(spark_session)

    assert df.schema == answer.schema, "Schema check for answer_df_2 failed."
    assert set(df.collect()) == set(answer.collect()), "Value check for answer_df_2 failed."  # noqa

