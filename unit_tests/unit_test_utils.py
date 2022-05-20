"""
Table of Contents

1. empty_dataframe: An example to show how a DataFrame can be built from scratch.
2. answer_check: Assert the answer calculated equals the answer expected.
3. assertion_helper: Assert that the value of object x equals the value of object y.
"""

from pyspark.sql import types as T, DataFrame


def empty_dataframe(spark_session) -> DataFrame:
    """
    An example to show how a DataFrame can be built from scratch.

    Input
        spark_session: Global variable available to all functions for testing.

    Output
        Small DataFrame example.
    """
    # Create a small schema to remind us of the format.
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
    ])

    # Create a nested list of rows of data to remind us of the format.
    DATA = [
        ["a"],
    ]

    # Return the newly created DataFrame as a reminder of the format.
    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def answer_check(
    df: DataFrame,
    answer: dict,
    col_to_check: str,
    extra_msg: str = "",
    key_col: str = "key",
    tol: float = None
) -> None:
    """
    Assert the answer calculated equals the answer expected.

    Inputs
        df: DataFrame being tested.
        answer: Dictionary with the DataFrame's primary key as the key and the correct outcome as the value.
        col_to_check: Column's name to compare values against.
        extra_msg: Any extra helpful text to add. Will appear in the details if the test fails.
        key_col: Name of DataFrame's key column.
        tol: If float values are used, rather than a direct equality, ensure values are within a tolerance.
    """
    # First, ensure the DF has the required column.
    assert col_to_check in df.columns, f"Column {col_to_check} not found; {extra_msg}"

    # Collect the DF to loop over all values.
    df_collect = df.collect()  # noqa
    for row in df_collect:
        # Look up the expect outcome based on the user's defined solution.
        key = row[key_col]
        expected = answer[key]

        # Find the computed result in the DF.
        computed = row[col_to_check]

        # Assert equality.
        assertion_helper(
            computed,
            expected,
            f"Computed {computed} but expected {expected} in row {key}; {extra_msg}.",
            tol
        )


def assertion_helper(x: object, y: object, message: str = "", tol: float = None) -> None:
    """
    Assert that the value of object x equals the value of object y. If tol is not None, then it will ensure the two
    values have an absolute difference less than tol.

    Inputs
        x: Value to compare against y.
        y: Value to compare against x.
        message: Optional string to display if the assertion is False.
        tol: Tolerance value if float values are involved. None means equality (==) is tested.
    """
    # Comparison. Strict equality for strings, integers, etc. Tolerance bounds check for floats.
    if tol is None:
        check = x == y
    else:
        if (x is None and y is not None) or (x is not None and y is None):
            # Exactly one of x or y is None. Error.
            check = False
        elif x is None and y is None:
            # Both are None, allowed.
            check = True
        else:
            # Neither are None, compare.
            check = abs(x - y) < tol

    # A True assertion will continue through; a False will end the check and result in a Failure in the Checks.
    assert check, message
