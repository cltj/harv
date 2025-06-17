import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql import Row
from harv.raw.flatten import flatten

@pytest.fixture(scope="module")
def spark():
    """Fixture to initialize a SparkSession."""
    return SparkSession.builder.master("local[1]").appName("pytest-pyspark-local-testing").getOrCreate()

def test_flatten_struct(spark):
    """Test flattening a DataFrame with nested struct fields."""
    # Create a nested DataFrame
    data = [
        Row(id=1, info=Row(name="Alice", age=25)),
        Row(id=2, info=Row(name="Bob", age=30)),
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("info", StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]), True),
    ])
    df = spark.createDataFrame(data, schema)

    # Flatten the DataFrame
    flattened_df = flatten(df)

    # Expected schema and data
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("info_name", StringType(), True),
        StructField("info_age", IntegerType(), True),
    ])
    expected_data = [
        (1, "Alice", 25),
        (2, "Bob", 30),
    ]

    # Assert schema and data
    assert flattened_df.schema == expected_schema
    assert flattened_df.collect() == [Row(*row) for row in expected_data]

def test_flatten_array(spark):
    """Test flattening a DataFrame with nested array fields."""
    # Create a nested DataFrame
    data = [
        Row(id=1, tags=[Row(tag="tag1"), Row(tag="tag2")]),
        Row(id=2, tags=[Row(tag="tag3")]),
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("tags", ArrayType(StructType([
            StructField("tag", StringType(), True),
        ])), True),
    ])
    df = spark.createDataFrame(data, schema)

    # Flatten the DataFrame
    flattened_df = flatten(df)

    # Expected schema and data
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("tags_tag", StringType(), True),
    ])
    expected_data = [
        (1, "tag1"),
        (1, "tag2"),
        (2, "tag3"),
    ]

    # Assert schema and data
    assert flattened_df.schema == expected_schema
    assert flattened_df.collect() == [Row(*row) for row in expected_data]

def test_flatten_no_nested_fields(spark):
    """Test flattening a DataFrame with no nested fields."""
    # Create a flat DataFrame
    data = [
        Row(id=1, name="Alice"),
        Row(id=2, name="Bob"),
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    # Flatten the DataFrame
    flattened_df = flatten(df)

    # Assert schema and data remain unchanged
    assert flattened_df.schema == df.schema
    assert flattened_df.collect() == df.collect()