
from pyspark.sql import types as T


# Map cast names as strings to PySpark types.
# Not an exhaustive list of types accepted by PySpark, but the common ones used.
SPARK_TYPE_MAP = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
    "boolean": T.BooleanType(),
    "date": T.DateType(),
}

