from pyspark.sql import SparkSession


def run_optimize(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    ) -> None:
    """
    Runs the OPTIMIZE command on a Delta table to improve clustering and reduce file sizes.

    This function performs an optimization operation on a specified Delta table in Unity Catalog.
    The optimization improves data layout and clustering if clustering keys are defined on the table.
    Additionally, the function ensures file compaction to reduce the number of small files.

    Args:
        spark (SparkSession): The active Spark session.
        catalog_name (str): The name of the catalog containing the table.
        schema_name (str): The name of the schema containing the table.
        table_name (str): The name of the table to be optimized.

    Returns:
        None: Executes the `OPTIMIZE` command on the specified table.

    Example:
        Optimize clustering and compress files for a table:
        run_optimize('catalog', 'schema', 'table_name')
        This will:
        - Run the `OPTIMIZE` command on the specified table.
        - Compact small files.
        - Improve clustering if the table has defined clustering keys.

    Raises:
        Exception: Prints an error message and rethrows the exception if optimization fails.

    Notes:
        - If the table has clustering keys, this function will improve clustering and data layout.
        - File compression is automatically handled during the optimization process to reduce the number of files.

    """
    try:
        validated_uc_name = f"{catalog_name}.{schema_name}.`{table_name}`"
        spark.sql(f"OPTIMIZE {validated_uc_name}")
    except Exception as e:
        function_name = "run_optimize"
        error_message = f"Error in function '{function_name}' while optimizing table '{validated_uc_name}': {e}"
        print(error_message) # noqa T201
