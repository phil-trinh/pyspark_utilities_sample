"""
Test the create_dataframe function in parsing/schema_ops.py.

create_dataframe(schema: T.StructField, data: list = None) -> DataFrame
"""

from collections import OrderedDict
from pyspark.sql import types as T, DataFrame

from ..unit_test_utils import answer_check

from transformations_library.parsing.schema_ops import create_dataframe, build_schema_from_dictionary
from transformations_library.library_params import SPARK_TYPE_MAP


def test_create_dataframe_0():
    """
    Test the function: ensure dataframe with schema is created.
    """
    # Create the schema for an empty data frame.
    header = OrderedDict()
    header["key"] = ["string", False]
    header["col1"] = ["int", True]
    header["col2"] = ["date", True]
    schema = build_schema_from_dictionary(header)

    # Create the empty DF.
    df = create_dataframe(schema)

    # Run assertions specific to checking the header.
    _private_assertions(df, header)


def test_create_dataframe_1():
    """
    Test the function: ensure dataframe with schema is created, giving it a list to populate the columns.
    """
    # Create the schema for an empty data frame.
    header = OrderedDict()
    header["key"] = ["string", False]
    header["col1"] = ["int", True]

    # Build the schema off the header.
    schema = build_schema_from_dictionary(header)

    # The nested list of data to send when creating.
    DATA = [
            ["a", 10],
            ["b", 24],
            ["c", None],
        ]

    # Create the non-empty DF.
    df = create_dataframe(schema, DATA)

    # The expected answer.
    answer = {"a": 10, "b": 24, "c": None}
    answer_check(df, answer, "col1", "test_1")

    # Run assertions specific to checking the header.
    _private_assertions(df, header)


def create_testing_dataframe(spark_session) -> DataFrame:
    """
    Create a DataFrame to use for testing this third version (_2) of the function.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.

    Output
        DataFrame for testing this function.
    """
    SCHEMA = T.StructType([
        T.StructField("key", T.StringType(), False),
        T.StructField("col1", T.IntegerType(), True),
    ])

    DATA = [
        ["a", 10],
        ["b", 24],
        ["c", None],
    ]

    return spark_session.createDataFrame(data=DATA, schema=SCHEMA)


def test_create_dataframe_2(spark_session):
    """
    Test the function: ensure dataframe with schema is created, giving it a DataFrame to populate the columns.

    Note: hard to test properly since a transform would use the TransformContext, ctx, rather than spark_session
        directly. However, I believe that ctx.spark_session from a transform will work correctly.

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    # Create the schema for an empty data frame.
    header = OrderedDict()
    header["key"] = ["string", False]
    header["col1"] = ["int", True]

    # Build the schema off the header.
    schema = build_schema_from_dictionary(header)

    # Use an independent method to create a data frame to ensure the RDD method works.
    DATA = create_testing_dataframe(spark_session)

    # Create the non-empty DF.
    df = create_dataframe(schema, DATA)

    # The expected answer.
    answer = {"a": 10, "b": 24, "c": None}
    answer_check(df, answer, "col1", "test_2")

    # Run assertions specific to checking the header.
    _private_assertions(df, header)


def _private_assertions(df: DataFrame, header: OrderedDict) -> None:
    """
    Private set of assertions for specifically checking the schema against the header.

    Inputs
        df: DataFrame.
        header: Expected schema information.
    """
    assert len(df.schema) == len(header), f"Number of columns expected {len(header)} but found {len(df.schema)}"

    for (s, h) in zip(df.schema, header):
        # Ensure the column is there in the correct order as the header.
        assert s.name == h, f"Column name {s.name} mismatch with {h}"

        # Get the type and nullability.
        data_type, nullable = header[h]

        # Convert type back into the string-style the user would provide.
        data_type_T = SPARK_TYPE_MAP[data_type]

        assert s.dataType == data_type_T, f"Column type for column {s.name} expected {data_type_T} but is {s.dataType}"
        assert s.nullable == nullable, f"Nullability for column {s.name} expected {nullable} but is {s.nullable}"


def test_create_dataframe_3():
    """
    Test the function: ensure empty dataframe with schema is created.
    """
    header = {"key": ["string", False], "col1": ["int", True]}
    schema = build_schema_from_dictionary(header)
    df = create_dataframe(schema, data=None)

    for s in df.schema:
        # Ensure the column is there.
        assert s.name in header, f"Column name {s.name} missing in header"

        # Get the type and nullability.
        data_type, nullable = header[s.name]
        # Convert type back into the string-style the user would provide.
        data_type_T = SPARK_TYPE_MAP[data_type]

        assert s.dataType == data_type_T, f"Column type for column {s.name} expected {data_type_T} but is {s.dataType}"
        assert s.nullable == nullable, f"Column type for column {s.name} expected {nullable} but is {s.nullable}"

