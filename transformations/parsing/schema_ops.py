"""
Table of Contents

1. select_and_alias: Select columns from a dataframe and alias them based on a dictionary mapping.
2. standard_case_columns: Convert column names to upper or lower case.
3. prevent_duplicate_names: Ensure two dataframes do not have duplicative column names before a join operation.
4. create_dataframe: Create a DataFrame with a specified schema and optional data.
5. build_schema_from_dictionary: Create a schema using a simplified interface.
"""

from collections import OrderedDict
from pyspark.sql import functions as F, types as T, DataFrame, SparkSession

from ..library_params import SPARK_TYPE_MAP


def select_and_alias(df: DataFrame, col_mapping: [dict, list]) -> DataFrame:
    """
    Select columns from a dataframe and alias them based on a dictionary mapping.

    Input
        df: DataFrame.
        col_mapping: Dictonary of desired columns to select from df (as keys)
                     and the corresponding alias names (as values).
                     If a list is given, the columns will not be renamed.

    Output
        DataFrame with selected columns, aliased to a new name.

    Example
        df = select_and_alias(df, {"soldier_name": "name"})
    """
    # If a dictionary is not given, create a mapping which will not rename the columns.
    if not isinstance(col_mapping, dict):
        col_mapping = {c: c for c in col_mapping}

    return df.select([F.col(c).alias(col_mapping[c]) for c in col_mapping])


def standard_case_columns(df: DataFrame, as_upper: bool = True) -> DataFrame:
    """
    Convert column names to upper or lower case.

    Input
        df: DataFrame.
        as_upper: True to set names to upper case; False to set names to lower case.

    Output
        DataFrame with all columns in the same case.

    Example
        df = standard_case_columns(df, as_upper=False)
        Will turn all column names to lower case.
    """
    # Column names will either be all upper case, or all lower case.
    if as_upper:
        mapping = {c: c.upper() for c in df.columns}
    else:
        mapping = {c: c.lower() for c in df.columns}

    return select_and_alias(df, mapping)


def prevent_duplicate_names(df1: DataFrame, df2: DataFrame, ignore: [str, list], append_value: str = None) -> DataFrame:
    """
    Ensure two DataFrames do not have duplicative column names before a join operation.
    Rename any columns in the second DataFrame that match.

    Inputs
        df1: DataFrame part of the join operation.
        df2: Other DataFrame part of the join operation. Columns, if necessary, will be renamed here only.
        ignore: String or list of columns not to compare.
        append_value: String to append to name of column to prevent column name duplication.
                      If None, the columns will be dropped from df2.

    Output
        Updated df2 with columns renamed to prevent duplication with df1.

    Example
        df2 = prevent_duplicate_names(df1, df2, ["col1", "col2"], "_dupe")
        Will rename any columns (except "col1" and "col2") in df2 by adding "_dupe" to the end.
    """
    if isinstance(ignore, str):
        ignore = [ignore]

    # List of columns in first DF.
    columns = [c for c in df1.columns if c not in ignore]
    # List of columns in second DF that are also found in the first DF.
    drop_or_rename = [c for c in df2.columns if c in columns]

    if append_value is None:
        # Drop the columns from df2.
        df2 = df2.drop(*drop_or_rename)
    else:
        # A value for append_value was provided, use it to rename, instead of dropping, these columns.
        for col in drop_or_rename:
            # Append a value to prevent duplicate names.
            base_name = col + append_value
            new_name = base_name

            # While there is still a conflict, append a number
            extra_duplication = 1
            while new_name in columns or new_name in df2.columns:
                new_name = f"{base_name}_{extra_duplication}"
                extra_duplication += 1

            # Found a name that does not cause a duplication. Rename.
            df2 = df2.withColumnRenamed(col, new_name)

    return df2


def create_dataframe(schema: T.StructField, data: [list, DataFrame] = None) -> DataFrame:
    """
    Create a DataFrame with a specified schema and optional data.

    Input
        schema: PySpark schema to use to build a DataFrame.
        data: Data to use to populate the DataFrame.
                If None, an empty DataFrame will be returned.
                If a list of lists is provided, the list will be converted by Spark.
                If a single DataFrame is provided, the underlying RDD will be used.

    Output
        DataFrame with the proper schema applied.

    Example
        df = create_dataframe(schema)
            Will create an empty DataFrame with that schema.

        df = create_dataframe(schema, old_df)
            Will extract the RDD of the old_df, apply the schema, and create a new DataFrame.
    """
    # Get access to the Spark Context methods.
    spark = SparkSession.builder.getOrCreate()

    # Create an empty DataFrame if the data input is left as None.
    if data is None:
        data = spark.sparkContext.emptyRDD()
    elif isinstance(data, DataFrame):
        # First, ensure only the columns desired in the final schema are selected.
        data = data.select(*schema.fieldNames())
        # Then, convert to RDD to apply the schema types as the next step.
        data = data.rdd

    return spark.createDataFrame(data=data, schema=schema)


def build_schema_from_dictionary(header_info: [OrderedDict, list]) -> T.StructType:
    """
    Create a schema using a simplified interface.

    Input
        header_info: Python dictionary where the keys will be the names of the columns.
            For the values of the dictionary:
                If a single value is provided, it must be the type of the column, nullable will be assumed True.
                    Valid column types are "string", "integer", "long", "double", "boolean", and "date".
                    If a type does not match the above list, it will get the default value of a StringType.
                If a list/tuple is provided, it should be of the form [column type, nullable].
                Note: an OrderedDict would be preferred over a standard dictionary.
            If a list is given rather than a dictionary, the column names will be the entries of the list, and
            all columns will be a StringType and nullable.

    Output
        T.StructType to be used to create a DataFrame.

    Example
        header = OrderedDict()
        header["key"] = ("string", False)
        header["column"] = ("int", True)
        schema = build_schema_from_dictionary(header)
    """
    # A list is allowed as a shortcut to get StringType, and nullable columns.
    if not isinstance(header_info, dict):
        # Create a temporary header to store the info.
        header_temp = OrderedDict()
        for col in header_info:
            header_temp[col] = ("string", True)

        # Overwrite the old list with this new dictionary.
        header_info = header_temp

    # Build the overall schema information, one column at a time.
    schema_list = []
    for column in header_info:
        if len(header_info[column]) == 2:
            # User provided both the type and the nullability of the column as the value.
            (data_type, nullable) = header_info[column]
        else:
            # Only the data type is given, assume nullability is True by default.
            data_type = header_info[column]
            nullable = True

        # Format is the name of the column, the type of the column, and if nulls are allowed.
        schema_list.append(T.StructField(column, SPARK_TYPE_MAP.get(data_type, T.StringType()), nullable))

    return T.StructType(schema_list)

