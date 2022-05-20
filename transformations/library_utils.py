"""
Table of Contents

1. str_or_list_to_dict: Combine a default value with a string or list input to return a dictionary.
"""


def str_or_list_to_dict(value: [str, list], default: str) -> dict:
    """
    Combine a default value with a string or list input to return a dictionary.

    Inputs
        value: Input values to operate on, options of a string or a list will add a default value to the output.
        default: String formatter of the form "{0}_your_text" or equivalent. Cannot be a f-style.

    Output
        Dictionary
            key will be the string or entries of the list given as input.
            value will be the name combined with the default value.

    Example
        str_or_list_to_dict("hello", "output_column_{0}") will return {"hello": "output_column_hello"}.
        str_or_list_to_dict(["hello", "world"], "new_{0}_col") will return
            {"hello": "new_hello_col", "world": "new_world_col"}.
    """
    # Already a dictionary, nothing further to do.
    if isinstance(value, dict):
        return value

    # Wrap as a list so the code does not iterate over the characters of the string, but rather uses the string itself.
    if isinstance(value, str):
        value = [value]

    # Use the default name provided by the user to give a consistent name across all inputs. It is required that the
    # input, default, has exactly one position to replace, i.e., "{0}" in the string.
    value_dict = {x: default.format(x) for x in value}

    return value_dict
