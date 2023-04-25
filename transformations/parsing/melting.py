"""
Table of Contents

1. melt: Convert DataFrame from wide to long format.
"""

from pyspark.sql import functions as F, DataFrame
from typing import Iterable


# hat tip to https://newbedev.com/how-to-melt-spark-dataframe
def melt(
    df: DataFrame,
    id_vars: Iterable[str] = None,
    value_vars: Iterable[str] = None,
    var_name: str = "categories",
    value_name: str = "values"
) -> DataFrame:
    """
    Convert DataFrame from wide to long format.

    Inputs
        df: DataFrame to melt.
        id_vars: Columns to keep "as is."
        value_vars: Columns to convert. Will be changed into two columns, one listing the column name, the other
                    listing the value.
        var_name: Name of new column containing the names of the original columns.
        value_name: Name of new column containing the values from the original columns.

    Output
        Updated DataFrame in long format.

    Example
        df = melt(df, ["col1", "col2"], ["col3", "col4"])
        Will convert DataFrame df as follows:
            Columns col1 and col2 will remain (with values repeated for the new rows).
            Two new columns will be added: var_name, and value_name.
                The values in var_name will be the names of the columns, literally col3 and col4.
                The values in value_name will be the values that appear in columns col3 and col4.
    """
    # If either id_vars or value_vars is None, assume that it is the full list of columns not in the other. But, both
    # cannot be None.
    if id_vars is None and value_vars is None:
        raise(ValueError, "Both id_vars and value_vars cannot be None!")
    elif id_vars is None:
        id_vars = [c for c in df.columns if c not in value_vars]
    elif value_vars is None:
        value_vars = [c for c in df.columns if c not in id_vars]

    # Create array<struct<variable: str, value: ...>>.
    _vars_and_vals = F.array(*(
        F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode.
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))
    cols = id_vars + [F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]

    return _tmp.select(*cols)

