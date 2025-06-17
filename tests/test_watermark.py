import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
from pyspark.sql.column import Column

# Import the functions under test
from harv.watermark import (
    convert_timestamp,
    get_last_watermark,
    get_df_last_watermark_values,
    set_df_last_watermark_values,
)


def test_convert_timestamp_naive():
    dt = datetime(2024, 12, 6, 12, 0, 0)
    result = convert_timestamp(dt, "UTC", "Europe/Oslo")
    assert result.tzinfo is not None
    assert result.tzinfo == ZoneInfo("Europe/Oslo")


def test_convert_timestamp_aware():
    dt = datetime(2024, 12, 6, 12, 0, 0, tzinfo=ZoneInfo("UTC"))
    result = convert_timestamp(dt, "UTC", "America/New_York")
    assert result.tzinfo == ZoneInfo("America/New_York")


def test_get_last_watermark_tbl_properties():
    spark = MagicMock(name="mock_spark")
    spark.conf.get.return_value = "UTC"  # spark.sql.session.timeZone
    tbl_properties_df = MagicMock(spec=DataFrame, name="tbl_properties_df")

    # Simulate a row returned from SHOW TBLPROPERTIES
    # 'last_ingested' key with a valid ISO timestamp
    tbl_properties_df.filter.return_value.select.return_value.first.return_value = Row(value="2024-12-06T12:00:00Z")
    spark.sql.side_effect = [
        tbl_properties_df,  # For SHOW TBLPROPERTIES
    ]

    result = get_last_watermark(spark, "catalog.schema.table", "ingested")
    assert result == datetime(2024, 12, 6, 12, 0, 0, tzinfo=timezone.utc)


def test_get_last_watermark_no_property():
    spark = MagicMock(name="mock_spark")
    spark.conf.get.return_value = "America/New_York"
    tbl_properties_df = MagicMock(spec=DataFrame, name="tbl_properties_df")
    tbl_properties_df.filter.return_value.select.return_value.first.return_value = None  # No property found

    # For the fallback, we need another query returning a row
    max_df = MagicMock(spec=DataFrame, name="max_df")
    max_df.first.return_value = Row(max_timestamp=datetime(2024, 12, 6, 15, 30, 0))  # naive datetime returned

    spark.sql.side_effect = [
        tbl_properties_df,  # SHOW TBLPROPERTIES
        max_df,             # SELECT MAX(...)
    ]

    result = get_last_watermark(spark, "catalog.schema.table", "ingested", "last_updated")
    # The result should be converted to America/New_York
    assert result.tzinfo == ZoneInfo("America/New_York")
    assert result.hour == 10  # If original was UTC 15:30, America/New_York is 5 hours behind UTC


def test_get_last_watermark_analysis_exception():
    spark = MagicMock(name="mock_spark")
    spark.conf.get.return_value = "UTC"
    # Raise AnalysisException on spark.sql
    spark.sql.side_effect = AnalysisException("Table not found")

    result = get_last_watermark(spark, "catalog.schema.nonexistent", "ingested")
    assert result == datetime(1970, 1, 1, 0, 0, 0)


@pytest.fixture
def mock_spark_functions():
    with patch("harv.watermark.col", autospec=True) as mock_col, \
         patch("harv.watermark._max", autospec=True) as mock_max:

        mock_col_inst = MagicMock(spec=Column, name="mock_col_inst")
        mock_col.return_value = mock_col_inst

        mock_max_col = MagicMock(spec=Column, name="mock_max_col")
        mock_max.return_value = mock_max_col

        yield {
            "col": mock_col,
            "max": mock_max,
            "col_inst": mock_col_inst,
            "max_col": mock_max_col
        }

@patch("harv.watermark.convert_timestamp", autospec=True)
def test_get_df_last_watermark_values(mock_convert_timestamp, mock_spark_functions):
    df = MagicMock(spec=DataFrame)
    df.columns = ["ingested", "last_updated"]

    df_agg = MagicMock(spec=DataFrame, name="agg_df")
    df.agg.return_value = df_agg

    df_agg.first.return_value = Row(ingested=datetime(2024,12,6,12,0,0), last_updated=None)

    timezone_mapping = {
        "ingested": {"source": "UTC", "target": "Europe/Oslo"},
        "last_updated": {"source": "UTC", "target": "America/New_York"}
    }

    def convert_side_effect(ts, src, tgt):
        if tgt == "Europe/Oslo":
            return datetime(2024,12,6,13,0,0, tzinfo=ZoneInfo("Europe/Oslo"))
        else:
            return None

    mock_convert_timestamp.side_effect = convert_side_effect

    result = get_df_last_watermark_values(df, timezone_mapping, "ingested", "last_updated")

    df.agg.assert_called_once()

    assert result["ingested"] == datetime(2024,12,6,13,0,0, tzinfo=ZoneInfo("Europe/Oslo"))
    assert result["last_updated"] is None


@patch("builtins.print", autospec=True)
def test_get_df_last_watermark_values_exception(mock_print):
    df = MagicMock(spec=DataFrame)
    df.agg.side_effect = Exception("Aggregation error")

    timezone_mapping = {
        "ingested": {"source": "UTC", "target": "Europe/Oslo"},
    }

    # The function doesn't re-raise, it prints and returns None by default
    result = get_df_last_watermark_values(df, timezone_mapping, "ingested")
    assert result is None
    mock_print.assert_called_once()
    assert "Aggregation error" in mock_print.call_args[0][0]


def test_set_df_last_watermark_values():
    spark = MagicMock(name="mock_spark")
    watermark_values = {
        "ingested": "2024-12-06T12:00:00",
        "last_updated": None
    }

    set_df_last_watermark_values(spark, "catalog.schema.table", watermark_values)

    spark.sql.assert_called_once()
    sql_called = spark.sql.call_args[0][0]
    assert "last_ingested" in sql_called
    assert "2024-12-06T12:00:00" in sql_called


@patch("builtins.print", autospec=True)
def test_set_df_last_watermark_values_exception(mock_print):
    spark = MagicMock(name="mock_spark")
    spark.sql.side_effect = Exception("SQL error")

    watermark_values = {
        "ingested": "2024-12-06T12:00:00",
    }

    set_df_last_watermark_values(spark, "catalog.schema.table", watermark_values)
    mock_print.assert_called_once()
    assert "SQL error" in mock_print.call_args[0][0]
