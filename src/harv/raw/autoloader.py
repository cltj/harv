import os
import re
from typing import Any, Callable, List, Optional, Tuple

from ..naming_standards import format_column

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


class AutoLoader:
    """Autoloader class for ingesting data."""

    valid_formats = ["csv", "json", "parquet", "delta"]

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        layer: str,
        source: str,
        storage_account_name: str,
        storage_container_name: str,
        file_listing_function: Callable[[str], List[Any]],
    ) -> None:
        """Initialize the AutoLoader class."""
        self.spark = spark
        self.catalog = catalog
        self.layer = layer
        self.source = source
        self.storage_account = storage_account_name
        self.storage_container = storage_container_name
        self.mount_source = f"abfss://{self.storage_container}@{self.storage_account}.dfs.core.windows.net"
        self.mount = f"{self.mount_source}/source/{self.source}"
        self.checkpoint_location = f"/Volumes/{self.catalog}/{self.layer}_{self.source}/meta/checkpoints"
        self.schema_location = f"/Volumes/{self.catalog}/{self.layer}_{self.source}/meta/schema"
        self.file_listing_function = file_listing_function

    def find_file_format(self, file_path: str) -> str:
        """
        Checks if the file format is valid.

        Args:
            file_path (str): Takes a file path.

        Returns:
            str: The file format if it is valid, otherwise None.

        """
        match = re.search(r"(?<=\.)\w+$", file_path)
        return match.group(0) if match and match.group(0) in self.valid_formats else None

    def get_paths(self, mount: str) -> list:  # can add table_name for glob finder
        """
        Recursivly crawls the paths of the usecase directory hierarchy and returns all paths that is not a directory.

        This list is used to generate the table names and file formats.

        Args:
            mount (str): The location of the storage account and container and source.

        Returns:
            list: All the paths that is not a directory.

        """
        path_list = []
        for item in self.file_listing_function(mount):
            if item.isDir():
                path_list.extend(self.get_paths(item.path))
            else:
                path_list.append(mount)
                break
        return path_list

    def generate_table_info(self) -> List[Tuple[str, str, str]]:
        """
        Produces the relevant information in order to read and write dataframes.

        Returns:
            List[Tuple[str, str, str]]: Returns a list of tuples containing the table name, file format, and path.

        """
        table_info = []
        for path in self.get_paths(self.mount):
            for file in self.file_listing_function(path):
                file_format = self.find_file_format(file.path)
                if file_format is None:
                    continue
                break
            table_name = os.path.relpath(path, self.mount)
            print(f"loading table from source: {path} with {file_format} as {table_name}")  # noqa: T201
            table_info.append((table_name, file_format, path))
        return table_info

    def create_schema_and_volume(self) -> None:
        """
        Creates the schema and checkpoint location for the table.

        Returns:
            None: If successful it returns None.

        """
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.layer}_{self.source}")
        self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.layer}_{self.source}.meta")

    def write_to_dbws_table(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Writes a DataFrame using mergeSchema into the catalog.schema.table hierarchy in databricks.

        Sets the checkpoint location to the meta volume and triggers the write.

        Args:
            df (DataFrame): the data to write encapsulated in a DataFrame.
            table_name (str): the name of the table (with formated columns).

        Returns:
            DataFrame: the DataFrame after writing.

        """
        self.create_schema_and_volume()
        return (
            df.writeStream.option("checkpointLocation", f"{self.checkpoint_location}/{table_name}")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(f"{self.catalog}.{self.layer}_{self.source}.{table_name}")
            .awaitTermination()
        )

    def format_column(self, df: DataFrame, column_format_mode: Optional[str]) -> str:
        """
        Formats the column names of the DataFrame.

        Args:
            df (DataFrame): the dataframe
            column_format_mode (str): the mode of the column format.
                Defaults to 'light' which will only remove special characters and spaces.
                'full' will attempt to convert the column names to snake_case.

        Returns:
            str: formatted columns

        """
        for column in df.columns:
            data = df.withColumnRenamed(column, format_column(column, mode=column_format_mode))
        return data

    def read_stream(
        self,
        delimiter: str,
        table_name: str,
        file_format: str,
        path: str,
    ) -> DataFrame:
        """
        Reads from the stream of data in unity catalog.

        Reads according to the file format checkpoint and schema location and timestamp.

        Args:
            delimiter (str): the delimiter of the data that is read.
            table_name (str): the name of the table.
            file_format (str): the format of the file.
            path (str): the path of the file.
            column_format_mode (str): how the column names should be formatted. (full)
                Defaults to 'light' which will only remove special characters and spaces.

                'full' will attempt to convert the column names to snake_case.

        Returns:
            DataFrame: The data after reading.
            table_name (str): The name of the table (with formated columns).

        """
        data = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", file_format)  # if csv then header must be included
            .option("checkpointLocation", f"{self.checkpoint_location}/{table_name}")
            .option("cloudFiles.schemaLocation", f"{self.schema_location}/{table_name}")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("delimiter", delimiter)
            .load(path)
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("ingested_at", current_timestamp())
        )

        return data, table_name
