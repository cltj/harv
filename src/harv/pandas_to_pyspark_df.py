import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _pandas_to_spark(df: pd.DataFrame, spark: SparkSession) -> DataFrame:
    """
    Function that converts Pandas data types to PySpark data types.

    Args:
        df (pd.DataFrame): the input Pandas DataFrame to be converted
        spark (SparkSession): the Spark session object

    Returns:
        DataFrame: a PySpark DataFrame with the same schema as the input Pandas DataFrame

    """
    dtype_mapping = {
        "int64": LongType(),
        "int32": IntegerType(),
        "float64": DoubleType(),  # 15 digits of precision / 8 bytes
        "float32": FloatType(),  # 7 digits of precision / 4 bytes
        "object": StringType(),
        "bool": BooleanType(),
        "datetime64[ns]": TimestampType(),
        # "timedelta[ns]": LongType(),  # Pandas timedelta as long in nanoseconds
        "category": StringType(),  # Categories are mapped to strings
        "int8": ByteType(),
        "int16": ShortType(),
        "uint8": ShortType(),
        "uint16": IntegerType(),
        "uint32": LongType(),
        "uint64": LongType(),
    }
    schema = StructType(
        [
            StructField(col, dtype_mapping[str(dtype)], nullable=True)
            for col, dtype in zip(df.columns, df.dtypes, strict=True)
        ]
    )
    return spark.createDataFrame(df, schema=schema)
