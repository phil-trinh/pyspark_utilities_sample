"""
Table of Contents

1. join_assertion: Function decorator to assert that the DataFrame does not change in row count after a join operation.
"""

import functools
import types


def join_assertion(only_one_output: bool = True) -> types.FunctionType:
    """
    Function decorator to assert that the DataFrame does not change in row count after a join operation.

    Assumptions:
        The decorated function takes as the first input the DataFrame in question, and the first output is the newly
        joined DataFrame. The assertion is that the number of rows does not change with this join step.
        Note: use this primarily to ensure a left-join keeps the same number of rows.

    Disclaimer:
        It may be possible the row count does not change because the same number of rows that are added balance the ones
        dropped. A future version should count the number by primary key to ensure the stability for other types of
        joins, e.g., inner.

    Inputs
        only_one_output: Indicates if the decorated function returns only the updated DataFrame or multiple outputs.

    Output
        Wrapped and decorated function.

    Example
        @join_assertion()
        df = my_function(df, df_join)
            Will call my_function as normal, but also assert that df does not change in row count.
    """
    def decorator(fun):
        """
        Nested function handles accepting and returning a function to decorate.
        """
        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            """
            Interacts with the decorated function taking a variable number of inputs and returning the results.
            """
            # Assumes DataFrame to check is first input.
            count = args[0].count()

            # Call decorated function.
            output = fun(*args, **kwargs)

            # Assumes the only, or first if multiple, output is the returned DataFrame.
            if only_one_output:
                new_count = output.count()
            else:
                new_count = output[0].count()

            # Raise assertion error if the row count changes.
            assert count == new_count, f"Row count changed from {count} to {new_count} in {fun.__name__}"

            # Return results from function fun.
            return output

        # Return wrapped function.
        return wrapper

    # Return decorated function.
    return decorator

