import pytest
import pandas as pd
from pyspark.sql import SparkSession
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
from harv.pandas_to_pyspark_df import _pandas_to_spark

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest-pyspark-local-testing").getOrCreate()

@pytest.mark.filterwarnings("ignore:distutils Version classes are deprecated. Use packaging.version instead.")
@pytest.mark.filterwarnings("ignore:Signature.*does not match any known type:UserWarning")
def test_pandas_to_spark(spark):
    # Create a pandas DataFrame with various data types
    data = {
        "int64_col": [1, 2, 3],
        "int32_col": pd.Series([1, 2, 3], dtype="int32"),
        "float64_col": [1.1, 2.2, 3.3],
        "float32_col": pd.Series([1.1, 2.2, 3.3], dtype="float32"),
        "object_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "datetime64_col": pd.to_datetime(["2021-01-01", "2021-01-02", "2021-01-03"]),
        "category_col": pd.Series(["a", "b", "c"], dtype="category"),
        "int8_col": pd.Series([1, 2, 3], dtype="int8"),
        "int16_col": pd.Series([1, 2, 3], dtype="int16"),
        "uint8_col": pd.Series([1, 2, 3], dtype="uint8"),
        "uint16_col": pd.Series([1, 2, 3], dtype="uint16"),
        "uint32_col": pd.Series([1, 2, 3], dtype="uint32"),
        "uint64_col": pd.Series([1, 2, 3], dtype="uint64"),
    }
    pdf = pd.DataFrame(data)

    sdf = _pandas_to_spark(pdf, spark)

    expected_schema = StructType([
        StructField("int64_col", LongType(), nullable=True),
        StructField("int32_col", IntegerType(), nullable=True),
        StructField("float64_col", DoubleType(), nullable=True),
        StructField("float32_col", FloatType(), nullable=True),
        StructField("object_col", StringType(), nullable=True),
        StructField("bool_col", BooleanType(), nullable=True),
        StructField("datetime64_col", TimestampType(), nullable=True),
        StructField("category_col", StringType(), nullable=True),
        StructField("int8_col", ByteType(), nullable=True),
        StructField("int16_col", ShortType(), nullable=True),
        StructField("uint8_col", ShortType(), nullable=True),
        StructField("uint16_col", IntegerType(), nullable=True),
        StructField("uint32_col", LongType(), nullable=True),
        StructField("uint64_col", LongType(), nullable=True),
    ])


    assert sdf.schema == expected_schema

if __name__ == "__main__":
    pytest.main()