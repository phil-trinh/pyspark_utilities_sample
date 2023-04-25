"""
Test the build_schema_from_dictionary function in parsing/schema_ops.py.

build_schema_from_dictionary(header_info: [OrderedDict, list]) -> T.StructType
"""

from collections import OrderedDict

from transformations_library.parsing.schema_ops import build_schema_from_dictionary
from transformations_library.library_params import SPARK_TYPE_MAP


def test_build_schema_from_dictionary_0():
    """
    Test the function: ensure schema is created.
    """
    # Create a header that needs to be a schema.
    header = OrderedDict()
    header["key"] = ["string", False]
    header["col1"] = ["int", False]
    header["col2"] = ["date", True]

    # Now, build the schema.
    schema = build_schema_from_dictionary(header)

    # Run the assertions specific to this function.
    _private_assertions(schema, header)


def test_build_schema_from_dictionary_1(spark_session):
    """
    Test the function: ensure dataframe with schema is created.

    Note: hard to test properly since a transform would use the TransformContext, ctx, rather than spark_session
        directly. However, I believe that ctx.spark_session from a transform will work correctly. Should be verified!

    Input
        spark_session: Globally available variable to build a DataFrame from scratch.
    """
    # Create a header that needs to be a schema.
    header = ["key", "col1", "col_2"]

    # Now, build the schema.
    schema = build_schema_from_dictionary(header)

    # Create the "real" header instead of the shortcut.
    header_full = {col: ("string", True) for col in header}

    # Run the assertions specific to this function.
    _private_assertions(schema, header_full)


def _private_assertions(schema, header: dict) -> None:
    """
    Private set of assertions for specifically checking the schema against the header.

    Inputs
        schema: DataFrame schema.
        header: Expected schema information.
    """
    # Create a dictionary to count the number of times each column was used.
    was_found = {col: 0 for col in header}

    for s in schema:
        # Ensure the column is there.
        assert s.name in header, f"Column name {s.name} missing in header"

        # Update dictionary to record the column was found.
        was_found[s.name] += 1

        # Get the type and nullability.
        data_type, nullable = header[s.name]

        # Convert type back into the string-style the user would provide.
        data_type_T = SPARK_TYPE_MAP[data_type]

        assert s.dataType == data_type_T, f"Column type for column {s.name} expected {data_type_T} but is {s.dataType}"
        assert s.nullable == nullable, f"Nullability for column {s.name} expected {nullable} but is {s.nullable}"

    for col in was_found:
        # Assert each column was found once.
        assert was_found[col] == 1, f"Expected {col} to be found once, was found {was_found[col]} times, {was_found}, {schema}"

