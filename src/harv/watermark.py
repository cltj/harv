
from datetime import datetime
from zoneinfo import ZoneInfo

from dateutil import parser
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as _max
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException


def convert_timestamp(
    timestamp: datetime,
    source_timezone: str,
    target_timezone: str,
    ) -> TimestampType:
    """
    Converts a datetime object from one timezone to another.

    This function takes a datetime object and converts it from a source timezone to a target timezone.
    If the input datetime object is naive (does not have timezone information), it assumes the source timezone.

    Args:
        timestamp (datetime): The datetime object to be converted.
        source_timezone (str): The source timezone as a string (e.g., 'UTC', 'Europe/Oslo').
        target_timezone (str): The target timezone as a string (e.g., 'UTC', 'America/New_York').

    Returns:
        datetime: A datetime object converted to the target timezone.

    Example:
        Convert a naive datetime from UTC to 'Europe/Oslo':
        from datetime import datetime
        from zoneinfo import ZoneInfo

        dt = datetime(2024, 12, 6, 12, 0, 0)  # Naive datetime
        converted_dt = convert_timestamp(
            timestamp=dt,
            source_timezone='UTC',
            target_timezone='Europe/Oslo'
        )
        Output: datetime object in 'Europe/Oslo' timezone.

    Notes:
        - If the input `timestamp` is naive (lacks timezone information), it is assumed to be in the `source_timezone`.
        - The function uses `ZoneInfo` from the `zoneinfo` module to handle timezone conversions.

    Raises:
        ValueError: If the provided timezone strings are invalid.

    """
    try:
        if timestamp.tzinfo is None:
            timestamp_utc = timestamp.replace(tzinfo=ZoneInfo(source_timezone))
        else:
            timestamp_utc = timestamp.astimezone(ZoneInfo(source_timezone))
    except ValueError:
        error_message = f"Invalid timezone provided: source='{source_timezone}', target='{target_timezone}'"
        raise ValueError(error_message) # noqa TRY002
    return timestamp_utc.astimezone(ZoneInfo(target_timezone))


def get_last_watermark(
    spark: SparkSession,
    uc_table_name: str,
    watermark_column: str,
    change_timestamp: str = None,
    ) -> TimestampType:
    """
    Retrieves the last watermark value for a Delta table and converts it to the Spark session's timezone.

    This function retrieves the last processed watermark from a Delta table by checking the table's TBLPROPERTIES
    or querying the maximum value of the specified watermark column. If the retrieved timestamp is not in UTC,
    it is converted to UTC and then adjusted to the Spark session's configured timezone.

    Args:
        spark (SparkSession): The active Spark session.
        uc_table_name (str): The full name of the Delta table in Unity Catalog format.
        watermark_column (str): The name of the column used for watermarking.
        change_timestamp (str, optional): An optional fallback column for retrieving the maximum timestamp
                                          if the watermark property is not found. Defaults to `None`.

    Returns:
        datetime: The last watermark value as a timezone-aware datetime object in the Spark session's timezone.

    Example:
        Retrieve the last watermark for a Delta table:
        last_watermark = get_last_watermark(
            spark=spark,
            uc_table_name='catalog.schema.table',
            watermark_column='ingested',
            change_timestamp='last_updated'
        )
        This will:
        - Check the table's TBLPROPERTIES for `last_ingested`.
        - If unavailable, query the maximum value of `last_updated`.

    Notes:
        - If the `last_<watermark_column>` property exists in TBLPROPERTIES, its value is parsed as an ISO timestamp.
        - If the property is missing, the function queries the maximum value of the fallback column (`change_timestamp`
          or `watermark_column`).
        - The timestamp is converted to the Spark session's configured timezone using `spark.sql.session.timeZone`.
        - If parsing or retrieval fails, a default timestamp of `1970-01-01 00:00:00` is returned.

    Raises:
        ValueError: If a watermark value cannot be parsed or converted.
        AnalysisException: If the table or column does not exist or the query fails.

    """
    spark_timezone = spark.conf.get("spark.sql.session.timeZone")
    spark_zone_info = ZoneInfo(spark_timezone)
    default_value = datetime(1970, 1, 1, 0, 0, 0) #noqa DTZ001

    try:
        tbl_properties = spark.sql(f"SHOW TBLPROPERTIES {uc_table_name}")

        watermark_property = f"last_{watermark_column}"
        row = tbl_properties.filter(tbl_properties["key"] == watermark_property).select("value").first()

        if row and row.value:
            try:
                last_watermark = parser.isoparse(row.value)
            except ValueError:
                error_message = f"""Warning:
                Unable to parse watermark property '{watermark_property}' value '{row.value}'.
                Using default timestamp."""
                raise ValueError(error_message)
            else:
                return last_watermark

        fallback_column = change_timestamp if change_timestamp else watermark_column
        result = spark.sql(f"SELECT MAX({fallback_column}) AS max_timestamp FROM {uc_table_name}").first().max_timestamp

        if result is not None:
          try:
            if result.tzinfo is None:
              result_utc = result.replace(tzinfo=ZoneInfo("UTC"))
            else:
              result_utc = result.astimezone(ZoneInfo("UTC"))
          except ValueError:
            error_message = f"""Warning:
            Unable to parse timestamp value '{result}'.
            Using default timestamp."""
            raise ValueError(error_message)
          return result_utc.astimezone(spark_zone_info)

    except AnalysisException as e:
        function_name = "get_last_watermark"
        error_message = f"Error in {function_name} for table '{uc_table_name}': {e}"
        print(error_message) # noqa T201
    return default_value


def get_df_last_watermark_values(
    df: DataFrame,
    timezone_mapping: dict,
    watermark_column: str,
    change_timestamp: str = None,
    ) -> dict:
    """
    Retrieves the maximum values for specified watermark columns in a DataFrame, adjusted for timezones.

    This function calculates the maximum values for the specified watermark and change timestamp columns in a
    DataFrame. The retrieved timestamps are converted between source and target timezones using a provided
    timezone mapping.

    Args:
        df (DataFrame): The input DataFrame to process.
        timezone_mapping (dict): A dictionary specifying timezone mappings for each column.
                                 Format: { "column_name": {"source": "source_timezone", "target": "target_timezone"} }.
        watermark_column (str): The name of the primary watermark column to process.
        change_timestamp (str, optional): An optional secondary timestamp column for processing. Defaults to `None`.

    Returns:
        dict: A dictionary with the maximum values for the specified columns. The keys are column names, and
              the values are the adjusted maximum timestamps, or `None` if no valid value exists.

    Example:
        Get adjusted maximum watermark values from a DataFrame:
        max_values = get_df_last_watermark_values(
            df=df,
            timezone_mapping={
                "ingested": {"source": "UTC", "target": "Europe/Oslo"},
                "last_updated": {"source": "UTC", "target": "America/New_York"}
            },
            watermark_column="ingested",
            change_timestamp="last_updated"
        )
        This will return a dictionary such as:
        {
            "ingested": datetime object in "Europe/Oslo" timezone,
            "last_updated": datetime object in "America/New_York" timezone
        }

    Notes:
        - The function dynamically determines which of the target columns (`watermark_column` and `change_timestamp`)
          exist in the DataFrame.
        - For each available column, it calculates the maximum value and adjusts it to the target timezone
          using the `convert_timestamp` function.
        - Columns with no valid maximum value will have `None` as their result.

    Raises:
        Exception: If any error occurs during aggregation, timestamp conversion, or processing.

    """
    try:
        target_columns = [watermark_column, change_timestamp]
        available_columns = [c for c in target_columns if c in df.columns]

        df_agg = df.agg(*(_max(col(c)).alias(c) for c in available_columns))

        row = df_agg.first()
        max_values = {
            c: (
                convert_timestamp(
                    row[c],
                    timezone_mapping[c]["source"],
                    timezone_mapping[c]["target"],
                    )
                if row[c] is not None else None
            )
            for c in available_columns
        }
    except Exception as e:
        function_name = "get_df_last_watermark_values"
        error_message = f"Error running function {function_name} for {target_columns}: {e}"
        print(error_message) # noqa T201
    else:
        return max_values


def set_df_last_watermark_values(
    spark: SparkSession,
    uc_table_name: str,
    watermark_values: dict,
    ) -> None:
    """
    Updates the last watermark values as table properties in a Delta table.

    This function sets the maximum watermark values for specified columns as TBLPROPERTIES in a Delta table.
    The watermark values are stored with the format `last_<column_name>` in the table's properties.

    Args:
        spark (SparkSession): The active Spark session.
        uc_table_name (str): The full name of the Delta table in Unity Catalog where TBLPROPERTIES are set.
        watermark_values (dict): A dictionary of column names and their respective maximum values.
                                 Format: { "column_name": "max_value" }.

    Returns:
        None: Executes SQL commands to set the TBLPROPERTIES for the specified table.

    Example:
        Set last watermark values for a Delta table:
        set_df_last_watermark_values(
            spark=spark,
            uc_table_name="catalog.schema.table",
            watermark_values={
                "ingested": "2024-12-06T12:00:00",
                "last_updated": "2024-12-06T15:30:00"
            }
        )
        This will set the following TBLPROPERTIES for the table:
        - 'last_ingested' = '2024-12-06T12:00:00'
        - 'last_last_updated' = '2024-12-06T15:30:00'

    Notes:
        - Only non-null watermark values are set as table properties.
        - This function is useful for tracking the last processed values for incremental data loading.

    Raises:
        Exception: If any error occurs while setting the table properties, an error message is printed and re-raised.

    """
    try:
        for column, last_value in watermark_values.items():
            if last_value:
                spark.sql(f"""
                ALTER TABLE {uc_table_name}
                SET TBLPROPERTIES ('last_{column}' = '{last_value}')
                """)
    except Exception as e:
        function_name = "set_df_last_watermark_values"
        error_message = f"""Error in {function_name} for table '{uc_table_name}'
                        and columns {list(watermark_values.keys())}: {e}"""
        print(error_message) # noqa T201
