from unittest.mock import MagicMock
import pytest
from pyspark.sql import SparkSession
from harv.optimize.optimize import run_optimize

@pytest.fixture
def mock_spark():
    return MagicMock(spec=SparkSession)

def test_run_optimize_pytest_success(mock_spark):
    run_optimize(mock_spark, 'catalog', 'schema', 'table_name')
    mock_spark.sql.assert_called_once_with("OPTIMIZE catalog.schema.`table_name`")

def test_run_optimize_pytest_exception(mock_spark, capsys):
    mock_spark.sql.side_effect = Exception('Pytest Exception')
    run_optimize(mock_spark, 'catalog', 'schema', 'table_name')
    captured = capsys.readouterr()
    assert "Error in function 'run_optimize' while optimizing table 'catalog.schema.`table_name`': Pytest Exception" in captured.out
