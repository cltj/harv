# import os
# import re
# import unittest
# from unittest.mock import patch, MagicMock, PropertyMock, call
# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.functions import col, current_timestamp
# from harv.autoloader import AutoLoader  # note: write_to_uc is not imported

# # Dummy file class to simulate the output of dbutils.fs.ls
# class FakeFile:
#     def __init__(self, path, is_dir):
#         self.path = path
#         self._is_dir = is_dir
#     def isDir(self):
#         return self._is_dir

# # Dummy callable that mimics dbutils.fs.ls behavior
# def dummy_ls(path: str):
#     # For a given mount point, return a directory and a file.
#     if path == "dummy_mount":
#         return [FakeFile("dummy_mount/subdir", True), FakeFile("dummy_mount/file1.csv", False)]
#     # For directories, return a file with a valid extension.
#     elif "subdir" in path:
#         return [FakeFile(f"{path}/file2.json", False)]
#     return []

# class TestAutoLoaderInitialization(unittest.TestCase):
#     def setUp(self):
#         self.spark = SparkSession.builder.appName("test_autoloader").getOrCreate()
#         self.catalog = "test_catalog"
#         self.layer = "test_layer"
#         self.source = "test_source"
#         self.storage_account_name = "test_storage_account"
#         self.storage_container_name = "test_storage_container"
#         # Use dummy_ls as the file_listing_function callable
#         self.file_listing_function = dummy_ls

#     def test_initialization(self):
#         al = AutoLoader(
#             self.spark,
#             self.catalog,
#             self.layer,
#             self.source,
#             self.storage_account_name,
#             self.storage_container_name,
#             self.file_listing_function
#         )
#         self.assertEqual(al.spark, self.spark)
#         self.assertEqual(al.catalog, self.catalog)
#         self.assertEqual(al.layer, self.layer)
#         self.assertEqual(al.source, self.source)
#         self.assertEqual(al.storage_account, self.storage_account_name)
#         self.assertEqual(al.storage_container, self.storage_container_name)
#         self.assertEqual(al.mount_source, f"abfss://{self.storage_container_name}@{self.storage_account_name}.dfs.core.windows.net")
#         self.assertEqual(al.mount, f"abfss://{self.storage_container_name}@{self.storage_account_name}.dfs.core.windows.net/source/{self.source}")
#         self.assertEqual(al.checkpoint_location, f"/Volumes/{self.catalog}/{self.layer}_{self.source}/meta/checkpoints")
#         self.assertEqual(al.schema_location, f"/Volumes/{self.catalog}/{self.layer}_{self.source}/meta/schema")

#     def test_find_file_format(self):
#         al = AutoLoader(
#             self.spark,
#             self.catalog,
#             self.layer,
#             self.source,
#             self.storage_account_name,
#             self.storage_container_name,
#             self.file_listing_function
#         )
#         test_cases = [
#             ("/path/to/file/data.csv", "csv"),
#             ("/path/to/file/data.json", "json"),
#             ("/path/to/file/data.parquet", "parquet"),
#             ("/path/to/file/data.delta", "delta"),
#             ("/path/to/file/data.txt", None),
#             ("/path/to/file/data", None),
#             ("/path/to/file/data.backup.parquet", "parquet"),
#         ]
#         for file_path, expected in test_cases:
#             result = al.find_file_format(file_path)
#             self.assertEqual(result, expected)

#     def test_generate_table_info(self):
#         al = AutoLoader(
#             self.spark,
#             self.catalog,
#             self.layer,
#             self.source,
#             self.storage_account_name,
#             self.storage_container_name,
#             self.file_listing_function
#         )
#         # Override get_paths to return a fixed path
#         al.get_paths = lambda mount: ["dummy_mount/subdir"]
#         table_info = al.generate_table_info()
#         # Normalize the expected table name using os.path.relpath and replace backslashes with forward slashes
#         expected_table_name = os.path.relpath("dummy_mount/subdir", al.mount).replace("\\", "/")
#         # Also normalize the generated table name in table_info
#         normalized_info = [(t.replace("\\", "/"), f, p.replace("\\", "/")) for t, f, p in table_info]
#         expected = [(expected_table_name, "json", "dummy_mount/subdir")]
#         self.assertEqual(normalized_info, expected)

#     @patch('harv.autoloader.SparkSession.sql')
#     def test_create_schema_and_volume(self, mock_spark_sql):
#         al = AutoLoader(
#             self.spark,
#             self.catalog,
#             self.layer,
#             self.source,
#             self.storage_account_name,
#             self.storage_container_name,
#             self.file_listing_function
#         )
#         al.create_schema_and_volume()
#         mock_spark_sql.assert_any_call(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.layer}_{self.source}")
#         mock_spark_sql.assert_any_call(f"CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.layer}_{self.source}.meta")


#     @patch('harv.autoloader.SparkSession.sql')
#     @patch('pyspark.sql.streaming.StreamingQuery.awaitTermination', new_callable=MagicMock)
#     @patch('pyspark.sql.streaming.DataStreamWriter.toTable', new_callable=MagicMock)
#     @patch('pyspark.sql.streaming.DataStreamWriter.trigger', new_callable=MagicMock)
#     @patch('pyspark.sql.streaming.DataStreamWriter.option', new_callable=MagicMock)
#     @patch('pyspark.sql.DataFrame.writeStream', new_callable=PropertyMock)
#     def test_write_to_dbws_table(self, mock_write_stream, mock_option, mock_trigger, mock_toTable, mock_awaitTermination, mock_spark_sql):
#         al = AutoLoader(
#             self.spark,
#             self.catalog,
#             self.layer,
#             self.source,
#             self.storage_account_name,
#             self.storage_container_name,
#             self.file_listing_function
#         )

#         pass


#     @patch('harv.autoloader.format_column')
#     def test_format_column(self, mock_format_column):
#         al = AutoLoader(
#             self.spark,
#             self.catalog,
#             self.layer,
#             self.source,
#             self.storage_account_name,
#             self.storage_container_name,
#             self.file_listing_function
#         )

#         mock_df = MagicMock(spec=DataFrame)
#         mock_df.columns = ["Column One", "Column Two"]

#         # Mock the format_column function to return the column name in snake_case
#         mock_format_column.side_effect = lambda col, mode: col.lower().replace(" ", "_") if mode == "full" else col.replace(" ", "")

#         # Ensure withColumnRenamed returns the mock DataFrame itself
#         mock_df.withColumnRenamed.return_value = mock_df

#         result = al.format_column(mock_df, "full")

#         # Check that the columns were renamed correctly
#         mock_df.withColumnRenamed.assert_any_call("Column One", "column_one")
#         mock_df.withColumnRenamed.assert_any_call("Column Two", "column_two")

#         self.assertEqual(result, mock_df)



#     def test_read_stream(self):
#         pass



#     def tearDown(self):
#         self.spark.stop()

# if __name__ == "__main__":
#     unittest.main()