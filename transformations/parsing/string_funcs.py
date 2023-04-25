"""
Table of Contents

1. multi_column_formatter: Combine multiple column values into one string.
2. first_n_characters: Create new column with first N characters from selected column.
3. replace_characters: Replace leading and trailing characters in values of a column.
4. underscore_to_space: Replace _'s in values of a specified column with a space.
5. string_padding: Pad a string with a leading character to ensure the length is at least a specified amount.
"""

from pyspark.sql import functions as F, types as T, DataFrame
from typing import Callable

from ..library_utils import str_or_list_to_dict


def multi_column_formatter(df: DataFrame, new_column_name: str, lambda_fun: Callable, *columns: list) -> DataFrame:
    """
    Combine multiple column values into one string.

    Inputs
        df: DataFrame.
        new_column_name: Name of new column to add.
        lambda_fun: Callable lambda-style function.
                    Must work in conjuntion with the order the columns will be provided to this function.
                    Must return a string.
                    Note: unless otherwise handled, nulls will be returned as "None".
        columns: Variable list of column names.

    Output
        Updated DataFrame with new_column_name column added.

    Example
        To format a month and year column to be zero-padded:
            lambda_fun = lambda month, year: f"{month:03}-{year}"
            df = multi_column_formatter(df, "month_year", lambda_fun, "month_col", "year_col")
        Those two lines will combine to format the month_year column to 00m-yyyy format.
    """
    # Use the provided lambda-style function to generate a string return type.
    converter = F.udf(lambda_fun, T.StringType())

    # Add new column using the newly created UDF from the lambda-style function.
    df = df.withColumn(new_column_name, converter(*columns))

    return df


def first_n_characters(
    df: DataFrame,
    columns: [dict, list, str],
    num_chars: int,
    default_name: str = None
) -> DataFrame:
    """
    Create new column with first N characters from selected column.

    Inputs
        df: Dataframe.
        columns: Dictionary of columns of which to take the first N characters.
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        num_chars: Number of characters we want.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.
                      If left as None, the format will be "{column_name}_first_{num_chars}".

    Output
        Updated DataFrame with new column containing first N characters from the selected column

    Example
        df = first_n_characters(df, 3, {"existing_col": "new_column"})
        Will return dataframe with "new_column" added as the first 3 characters of the values in "existing_col".
    """
    # If not provide, the default name will include the  number of characters. Not required, of course, if user decides.
    if default_name is None:
        default_name = "{0}_first_{1}".format("{0}", num_chars)

    # Allowing for a single string or a list. Make sure it is a dictionary.
    columns = str_or_list_to_dict(columns, default_name)

    for col in columns:
        # Add new column containing first N characters from the selected column.
        df = df.withColumn(columns[col], F.col(col).substr(1, num_chars))

    return df


def replace_characters(
    df: DataFrame,
    columns: [dict, list, str],
    remove_chars: [dict, list, str] = " _",
    universal: bool = False,
    collapse: bool = False,
    default_name: str = "{0}",
    default_replacement: str = "",
) -> DataFrame:
    """
    Replace leading and trailing characters in values of a column.

    Inputs
        df: DataFrame.
        columns: Dictionary of columns of which to trim characters.
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        remove_chars: Dictionary of chars to replace from start and end of string.
                      Key represents the character at start or end to replace.
                      Value represents the character(s) to use as a replacement.
                      If a string or list, replacement will be the default_replacement keyword argument.
                      Note: Replacement characters must be a string with length 1.
        universal: Use False to only replace these characters at the start or end; stopping when a not-to-be-replaced
                   character is found.
                   Use True to replace all instances regardless of position.
        collapse: After replacement, will collapse consecutive instances into one.
                  Warning: may not work properly if the replacement character is a consecutive string, e.g., "~~". An
                  intermediate step may be necessary.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.
                      If left as None, the format will be "{column_name}_first_{num_chars}".
        default_replacement: If a list or string is given for remove_chars, the replacement value for all characters
                             needing replacement will default to this value.

    Output
        Updated DataFrame with characters in the specified columns replaced/removed.

    Examples
        df = replace_characters(df, {"values": "output"}, "_ ")
        Will remove all leading and trailing spaces or underscores from "values" column, creating "output".

        df = replace_characters(df, {"values": "output"}, {" ": "", "-": "_"}, universal=True)
        Will remove all spaces anywhere in the string, and will replace all - with _ anywhere in the string using the
        "values" column, creating "output".

        df = replace_characters(df, "values", {"_", " "}, universal=True)
        df = replace_characters(df, "values", {" ": ""})
        In combination, will first replace all _'s with a space, and then remove leading/trailing spaces. .
    """
    def _replacement(char_set: dict, universal: bool, from_start: bool, string: str) -> str:
        """
        Private function to implement the logic of string replacement. This easily allows for the possibility that the
        characters wished to be replaced are intertwined. For example "_-_-". Also, this allows for individual
        characters to have a unique replacement value - so this approach, while more code, seemed easier.

        Inputs
            char_set: Dictionary mapping a character, as key, to find and the set of characters, as value, as
                      replacements.
            universal: Use False to only replace these characters at the start or end; stopping when a not-to-be-
                       replaced character is found.
                       Use True to replace all instances regardless of position.
            from_start: Use True to search for characters at the beginnig of the string.
                        Use False to search for characters at the end of the string.
            string: Value to search for the replacement characters.

        Output
            String value after looking over the character set to replace.
        """
        # Null values should return null.
        if string is None:
            return None

        # Keep replacing until a not-to-be-replaced character is found.
        keep_replacing = True

        # Eventual output.
        new_string = ""

        # If searching for trailing characters, reverse the string and start at the beginning.
        if not from_start:
            string = string[::-1]

        # For each character in the string, evaluate it - comparing against the set of to-be-replaced characters.
        for char in string:
            if char in char_set:  # User indicated to replace it.
                if keep_replacing:  # Check that we are still replacing.
                    new_string += char_set[char]
                else:  # No longer replacing, keep original value.
                    new_string += char
            else:  # Not a character to be replaced.
                new_string += char  # Keep original value.
                if not universal:  # If universal, will replace them all. If not, stop replacing now.
                    keep_replacing = False

        # If string was reversed, reverse it again to get it back in the correct order.
        if not from_start:
            new_string = new_string[::-1]

        return new_string

    def _call_replacement(char_set: dict, universal: bool, from_start: bool) -> Callable:
        """
        Private function to return the UDF to do the character replacement, taking non-column inputs.

        Inputs
            char_set: Dictionary mapping a character, as key, to find and the set of characters, as value, as
                      replacements.
            universal: Use False to only replace these characters at the start or end; stopping when a not-to-be-
                       replaced character is found.
                       Use True to replace all instances regardless of position.
            from_start: Use True to search for characters at the beginnig of the string.
                        Use False to search for characters at the end of the string.

        Output
            The UDF which can be called on a column by supplying these extra (non-column) parameters.
        """

        return F.udf(lambda s: _replacement(char_set, universal, from_start, s), T.StringType())

    # Column input can be a string, list, or dictionary. Ensure a dictionary is used below.
    columns = str_or_list_to_dict(columns, default_name)

    # If replacement set of characters is not a dictionary, match each up with the default replacement character.
    if not isinstance(remove_chars, dict):
        remove_chars = {c: default_replacement for c in remove_chars}

    # Using Python string manipulation instead of regex as replacement characters could be intertwined.
    for col in columns:
        df = df.withColumn(columns[col], _call_replacement(remove_chars, universal, True)(F.col(col)))
        if not universal:
            df = df.withColumn(columns[col], _call_replacement(remove_chars, universal, False)(F.col(columns[col])))

        if collapse:
            # May have left consecutive instances - collapse into one value.
            for char in remove_chars:
                pat = f"([{remove_chars[char]}]+)"
                rep = f"{remove_chars[char]}"
                df = df.withColumn(columns[col], F.regexp_replace(F.col(columns[col]), pat, rep))

    return df


def underscore_to_space(df: DataFrame, columns: [dict, list, str], default_name: str = "{0}") -> DataFrame:
    """
    Replace _'s in values of a specified column with a space.

    Will also remove leading and trailing _'s and spaces. Consecutive _'s will be collapsed into one space.

    Inputs
        df: DataFrame.
        columns: Dictionary of columns of which to forward fill.
                 Key represents the name of the column to use in the computation.
                 Value represents the name of the column to create.
                 If a string or list, name will be created using the default_name keyword argument.
        default_name: If a list or single string is given for columns, the new column(s) will use this format.

    Output
        Updated DataFrame with _'s in a column replaced with a space.

    Example
        df = underscore_to_space(df, "col1")
        Will replace any _'s in the values of col1 with a space, overwriting col1. Consecutive _'s treated as one.
    """
    columns = str_or_list_to_dict(columns, default_name)
    df = replace_characters(df, columns, remove_chars={"_": " "}, universal=True, collapse=True)

    columns_updated = {columns[c]: columns[c] for c in columns}
    df = replace_characters(df, columns_updated, remove_chars={" ": ""})

    return df


def string_padding(df: DataFrame, column: str, width: int, character: str = "0") -> DataFrame:
    """
    Pad a string with a leading character to ensure the length is at least a specified amount.

    Inputs
        df: DataFrame.
        column: Name of column to pad.
        width: Minimum length of the string after padding.
        character: String to use for padding. Default is "0".

    Output
        Updated DataFrame.

    Example
        df = string_padding(df, "string_col", 3)
        Will replace "string_col" with a version with padding to 3 characters.
    """
    @F.udf(T.StringType())
    def _padding(value: str, width: int, character: str) -> str:
        """
        Pad a string with a leading character to ensure the length is at least a specified amount.

        Inputs
            value: String to pad.
            width: Minimum length of the string after padding.
            character: String to use for padding. Default is "0".

        Output
            String with padding in front to ensure a minimum length.
        """
        if value is None:
            return None

        return value.rjust(width, character)

    return df.withColumn(column, _padding(F.col(column), F.lit(width), F.lit(character)))

