import csv
from io import StringIO

import pandas as pd


class CSVParserError(Exception):
    """Raised when CSV parsing fails."""

    pass


def parse_csv(content: str, delimiter: str | None = None) -> pd.DataFrame:
    """
    Parse CSV content into a pandas DataFrame.

    Args:
        content (str): CSV data as a string.
        delimiter (str, optional): Force a specific delimiter. If not set, it will be auto-detected.

    Returns:
        pd.DataFrame: Parsed DataFrame.

    Raises:
        CSVParserError: If parsing fails.

    """
    if delimiter is None:
        try:
            sample = content[:2048]
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter
        except Exception:
            delimiter = ";"  # sensible fallback for e.g. Norwegian CSVs

    try:
        return pd.read_csv(StringIO(content), sep=delimiter)
    except Exception as e:
        error_message = f'Failed to parse CSV content with delimiter "{delimiter}": {e}'
        raise CSVParserError(error_message) from e
