import pytest
from harv.ingest.parsers import parse_csv, CSVParserError

def test_parse_csv_with_semicolon():
    content = "a;b;c\n1;2;3\n4;5;6"
    df = parse_csv(content)
    assert df.shape == (2, 3)
    assert list(df.columns) == ["a", "b", "c"]

def test_parse_csv_with_explicit_comma():
    content = "a,b\n1,2\n3,4"
    df = parse_csv(content, delimiter=",")
    assert df.shape == (2, 2)

def test_parse_csv_error_raises():
    malformed = 'a;b\n1;"unterminated quote\n3;4'
    with pytest.raises(CSVParserError):
        parse_csv(malformed)


