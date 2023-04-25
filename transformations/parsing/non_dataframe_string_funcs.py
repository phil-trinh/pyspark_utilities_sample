"""
Table of Contents

1. clean_name_for_header: Replace invalid characters and return a clean version.
2. camel_to_snake: Change value from camelCase to snake_case.
3. ensure_no_conflict: Ensure new column name does not conflict with existing columns.
"""

import re


def clean_name_for_header(
    value: str,
    as_lower: bool = False,
    as_upper: bool = False,
    collapse: bool = False,
    trim: bool = False
) -> str:
    """
    Replace invalid characters and return a clean version.

    Input
        value: String.
        as_lower: True to return a lower case version of the string; False to leave unchanged.
        as_upper: True to return an upper case version of the string; False to leave unchanged.
                  Note, as_lower takes precedence if both of these are set to True.
        collapse: True to replace consecutive underscores with one single.
        trim: True to remove leading or trailing underscores.

    Output
        String with invalid characters replaced.

    Example
        out = clean_name_for_header("This Is Not a Valid{Column}=Name")
        Will return "This_Is_Not_a_Valid_Column__Name".

        out = clean_name_for_header("[This Is Not a Valid{Column}=Name]", as_lower=True, collapse=True, trim=True)
        Will return "this_is_not_a_valid_column_name".
    """
    # Replace characters which are not allowed to be in a column's name.
    # Note: technically a . is allowed, but it fails when referenced, so replace it here to make life easier.
    for c in "~!@#$%^&*+\\'\" ,;:[]{}()\n\t=.":
        value = value.replace(c, '_')

    # Find consecutive underscores and replace with a single instance.
    if collapse:
        value = re.sub("([_])+", "_", value)

    # Find underscores that start (^) or end ($) the string and replace with an empty string (nothing).
    if trim:
        value = re.sub("^([_]+)", "", value)
        value = re.sub("([_]+)$", "", value)

    # If a specific case was requested, return in that form; otherwise leave unchanged.
    if as_lower:
        return value.lower()
    elif as_upper:
        return value.upper()
    else:
        return value


def camel_to_snake(value: str) -> str:
    """
    Change value from camelCase to snake_case.

    Input
        value: String.

    Output
        String with camel case replace with snake case.

    Example
        out = camel_to_snake("ThisIsCamelCaseConvertedToSnakeCase")
        Will return this_is_camel_case_converted_to_snake_case.
    """
    value = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', value)

    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', value).lower()


def ensure_no_conflict(existing: list, column: str) -> str:
    """
    Ensure new column name does not conflict with existing columns.

    Inputs
        existing: List of the names of columns in a DataFrame.
        column: Name of column to test for existence.

    Output
        String of a name causing no conflicts.

    Example
        output = ensure_no_conflict(["column1", "column2"], "column2")
        Will return "_column2_" to avoid conflicts with existing column names.
    """
    # Pad with _'s until that name does not exist in the current list of columns.
    while column in existing:
        column = "_" + column + "_"

    return column

