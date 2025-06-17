from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode
from pyspark.sql.types import ArrayType, StructType


def flatten(df: DataFrame) -> DataFrame:  # type: ignore
    """
    Flattens a nested dataframe by recursively traversing the schema.

    Parameters:
        df (DataFrame): The dataframe to be flattened.

    Returns:
        DataFrame: The flattened dataframe.

    Example:
        flatten_recursive(df = df)

    Raises:
        Exception: Prints an error message if the flattening fails.

    """
    try:
        while True:
            df, schema_changed = _process_schema(df)
            if not schema_changed:
                break
        return df  # noqa: TRY300
    except Exception as e:
        function_name = "flatten"
        msg = f"Error writing data in function {function_name}: {e}"
        print(msg)  # noqa: T201


def _process_schema(df: DataFrame) -> tuple[DataFrame, bool]:
    """
    Processes the schema of the dataframe to flatten nested fields.

    Parameters:
        df (DataFrame): The dataframe to process.

    Returns:
        tuple[DataFrame, bool]: The updated dataframe and a flag indicating if the schema changed.

    """
    for field in df.schema.fields:
        dtype = field.dataType
        name = field.name

        if isinstance(dtype, StructType):
            df = _flatten_struct(df, name, dtype)
            return df, True
        if isinstance(dtype, ArrayType):  # type: ignore
            df = _flatten_array(df, name, dtype)
            return df, True
    return df, False


def _flatten_struct(df: DataFrame, name: str, dtype: StructType) -> DataFrame:
    """
    Flattens a struct field in the dataframe.

    Parameters:
        df (DataFrame): The dataframe to process.
        name (str): The name of the struct field.
        dtype (StructType): The data type of the struct field.

    Returns:
        DataFrame: The updated dataframe.

    """
    for subfield in dtype.fields:
        df = df.withColumn(
            f"{name}_{subfield.name}",
            col(f"{name}.{subfield.name}"),  # type: ignore
        )
    return df.drop(name)


def _flatten_array(df: DataFrame, name: str, dtype: ArrayType) -> DataFrame:
    """
    Flattens an array field in the dataframe.

    Parameters:
        df (DataFrame): The dataframe to process.
        name (str): The name of the array field.
        dtype (ArrayType): The data type of the array field.

    Returns:
        DataFrame: The updated dataframe.

    """
    element_type = dtype.elementType
    df = df.withColumn(name, explode(col(name)))  # type: ignore
    if isinstance(element_type, StructType):  # type: ignore
        for subfield in element_type.fields:
            df = df.withColumn(
                f"{name}_{subfield.name}",
                col(f"{name}.{subfield.name}"),  # type: ignore
            )
        df = df.drop(name)
    return df
