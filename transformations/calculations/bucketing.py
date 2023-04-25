"""
Table of Contents

1. bucket: Bucketize a column based on numerical values.
"""

from pyspark.sql import functions as F, DataFrame


def bucket(df: DataFrame, column_map: dict, bucket_map: dict) -> DataFrame:
    """
    Place items into specified buckets.

    Inputs
        df: DataFrame.
        column_map: Dictionary of columns to bucketize.
                    Key is the name of column containing values to bucket.
                    Value is the name of new column to create.
        bucket_map: Dictionary containing the buckets. Keys need to be a string of the form:
                        "[" or "(" +
                        "lower, upper" +
                        "]" or ")"
                    Such that values will be bucketed according to the rules: [ or ] implies the endpoint is included,
                    and ( or ) implies the endpoint is not included; i.e., [lower, upper), means lower <= x < upper.
                    The dictionary's value for the key will be the string representing the name of the bucket.
                    Note: since the dictionary is unordered, it is assumed the intervals do not overlap.

    Output
        Updated DataFrame with the new column(s) added.

    Example
        buckets = {"(-inf, 0]": "low", "(0, inf)": high"}
        df = bucket(df, {"col_a": "col_a_buckets", "col_b": "col_b_buckets"}, buckets)
            Will add two new columns, each using the rule -inf < x <= 0 for "low" and 0 < x < inf for "high".
    """
    def _parse_bucket_interval(interval: str, column: str) -> tuple:
        """
        Private function to parse the interval into the conditions.

        Inputs
            interval: String format of the interval notation for the bucket.
            column: Name of column being tested when returning the Spark logic.

        Output
            Tuple containing the lower and upper conditions for the column.
        """
        # Remove spaces.
        bucket = interval.strip()

        # Get the interval conditions as the start and end of the string.
        lower_bound = bucket[0]
        upper_bound = bucket[-1]

        # Extract the values of the interval.
        (lower, upper) = bucket[1:-1].split(",")
        lower_value = float(lower)
        upper_value = float(upper)

        # Compute the lower bound condition. [ means >=, ( means >
        if lower_bound == "[":
            lower_condition = F.col(column) >= lower_value
        else:
            lower_condition = F.col(column) > lower_value

        # Compute the upper bound condition. ] means <=, ) means <
        if upper_bound == "]":
            upper_condition = F.col(column) <= upper_value
        else:
            upper_condition = F.col(column) < upper_value

        return (lower_condition, upper_condition)

    for column in column_map:
        new_column = column_map[column]
        temp_columns = []
        for interval in bucket_map:
            # Create a name for this temporary column from the buckets.
            this_col = f"temp_column_{interval}"
            this_col = this_col.replace(".", "_")
            temp_columns.append(this_col)

            # Parse the bucket's info, return the interval conditions.
            (lower_condition, upper_condition) = _parse_bucket_interval(interval, column)

            # If the entry falls within this bucket, give it a name, otherwise leave empty.
            df = (
                df
                .withColumn(
                    this_col,
                    F.when(lower_condition & upper_condition, bucket_map[interval])
                    .otherwise(F.lit(None).cast("string"))
                )
            )

        # With all of the columns now filled out, coalesce and remove the temporary ones.
        df = (
            df
            .withColumn(new_column, F.coalesce(*temp_columns))
            .drop(*temp_columns)
        )

    return df

