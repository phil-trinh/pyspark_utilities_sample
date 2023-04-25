"""
Table of Contents

1. split_list_to_columns: Split a list to separate columns.
2. get_distinct_list: Return the unique elements, ordered, into an array-type column.
"""

from pyspark.sql import functions as F, types as T, DataFrame, Row, SparkSession
from typing import Callable


def split_list_to_columns(
    data: DataFrame,
    col_index: str,
    col_target: str,
    dtype: Callable = str,
    dtype_target: Callable = T.StringType(),
    drop_col_target: bool = False
) -> DataFrame:
    """
    Split a list to separate columns.

    Inputs
        data: DataFrame.
        col_index: DataFrame's primary key column.
        col_target: Explodes array in this column to new columns.
        dtype: Type of values expected.
        dtype_target: PySpark schema type of values expected.
        drop_col_target: Boolean indicating whether to drop col_target column.
            Default: False (keep col_target); set to True if you want to drop the column.

    Output
        Updated DataFrame, including new columns resulting from
        exploding the original column of arrays (col_target).
            NOTE: New column names are based on the original column (col_target) name and
                  their location/index (starting at index 0) in the array.

    Example
        df = split_list_to_columns(df, "primary_key", "array_col", int, T.IntegerType())
        Will take arrays of integers from array_col and create new columns for the DataFrame,
        ranging from "array_col_0" to "array_col_{n-1}".
    """
    def explode_vals(x, ncols: int, dtype: Callable = str) -> list:
        """
        Returns row with length ncols. Fill with null values.

        Inputs
            x: Values. If not a list, will be stored as a single-valued list.
            ncols: Number of columns the length of output should have.
            dtype: Expected value type.

        Output
            Returns a list of values, padded with None to ensure length is ncols.

        Example
            output = explode_values([1, 2, 3], 4, int)
            Will return [1, 2, 3, None].
        """
        if isinstance(x, list):
            out = [dtype(x_) if x_ is not None else None for x_ in x]
        elif x is not None:
            out = [dtype(x)]
        else:
            out = [None]

        if len(x) < ncols:
            out.extend([None] * (ncols - len(x)))

        return out

    # subset to index and target columns; index column is not nullable
    tmp = data.select(col_index, col_target).withColumn('len', F.size(F.col(col_target)))
    ncols = tmp.agg(F.max(F.col('len')).alias('max_len')).collect()[0]['max_len']  # noqa

    # check for valid number of columns
    if ncols < 1:
        return data

    cols = ['{}_{}'.format(col_target, i) for i in range(ncols)]
    print("Splitting {} into {} columns".format(col_target, ncols))

    # create schema
    schema = T.StructType(
        [T.StructField(col_index, T.StringType(), False)]
        + [T.StructField(col_name, dtype_target, True) for col_name in cols]
    )

    # use rdd map function to split list into multiple columns
    rdd = (
        tmp
        .where(F.col('len') > 0)
        .rdd
        .map(lambda x: Row(str(x[0]), *explode_vals(x[1], ncols, dtype=dtype)))
    )

    # create spark dataframe
    spark = SparkSession.builder.getOrCreate()
    tmp_flat = spark.createDataFrame(data=rdd, schema=schema)

    data = data.join(tmp_flat, on=col_index, how='left')

    # drop column if drop_col_target set to True
    if drop_col_target:
        data = data.drop(col_target)

    return data


def get_distinct_list(df: DataFrame, column_map: dict) -> DataFrame:
    """
    Return the unique elements, ordered, into an array-type column.

    Inputs
        df: DataFrame.
        column_map: Dictionary of columns on which to operate.
                    Key represents the name of the column to use.
                    Value represents the Spark type of the values.
                    If a string or list of strings is provided, then the array will assume the array is filled with
                    StringType().

    Output
        DataFrame with column(s) updated.

    Example
        df = get_distinct_list(df, {"column_a": IntegerType()})
    """
    def find_distinct_elements(data: list) -> list:
        """
        Return the distinct list of values from a list.

        Input
            data: List of values.

        Output
            List of distinct values.
        """
        if data and len(data) > 0:
            output = list(set(data))
            output.sort()
        else:
            output = None

        return output

    # Ensure input is a dictionary.
    if isinstance(column_map, str):
        column_map = [column_map]
    if not isinstance(column_map, dict):
        column_map = {col: T.StringType() for col in column_map}

    # Use the above UDF to pull out distinct elements from an array.
    for column in column_map:
        find_distinct_udf = F.udf(find_distinct_elements, T.ArrayType(column_map[column]))
        df = df.withColumn(column, find_distinct_udf(column))

    return df

