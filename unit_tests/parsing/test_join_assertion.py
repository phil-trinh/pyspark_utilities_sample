"""
Test the join_assertion function in parsing/assertions.py.

join_assertion(only_one_output: bool = True) -> types.FunctionType
"""

from pyspark.sql import types as T, DataFrame

from transformations_library.parsing.assertions import join_assertion


def create_primary_df(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
    ])

    DATA = [
        ["a"],
        ["b"],
        ["c"],
        ["d"],
        ["e"],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def create_good_join_df(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("col_a", T.StringType(), True),
    ])

    DATA = [
        ["a", "hello"],
        ["b", "world"],
        ["c", "how"],
        ["d", "are"],
        ["e", "you"],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def create_bad_join_df(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("col_a", T.StringType(), True),
    ])

    DATA = [
        ["a", "hello"],
        ["b", "world"],
        ["d", "how"],
        ["d", "are"],  # Note the duplication of d.
        ["e", "you"],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


@join_assertion()
def my_join_function(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    A small function to join df2 onto df1 as a left join, to test if the row count is the same for df1.
    """
    return df1.join(df2, on="key", how="left")


def test_join_assertion_0(spark_session):
    """
    Test the function: perform a good join.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df1 = create_primary_df(spark_session)
    df2 = create_good_join_df(spark_session)
    try:
        df1 = my_join_function(df1, df2)
    except AssertionError:
        raise AssertionError("Should have been a good join.")


def test_join_assertion_1(spark_session):
    """
    Test the function: perform a bad join.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    df1 = create_primary_df(spark_session)
    df2 = create_bad_join_df(spark_session)
    try:
        df1 = my_join_function(df1, df2)
    except AssertionError as err:
        # Verify that the correct AssertionError was returned. If the message matches, it is okay.
        msg = err.__str__()
        assert msg == "Row count changed from 5 to 6 in my_join_function", "Incorrect fail message returned."

