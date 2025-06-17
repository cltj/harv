import pytest
from harv.naming_standards import format_column

def test_format_column_light():
    # Test with regular input
    assert format_column("ExampleName", "AnotherExample", mode="light") == "ExampleName_AnotherExample"
    # Test with special characters
    assert format_column("Example;Name", "Another,Example", mode="light") == "Example_Name_Another_Example"
    # Test with empty string
    assert format_column("", mode="light") != ""
    # Test with multiple underscores
    assert format_column("Example__Name", "Another__Example", mode="light") == "Example_Name_Another_Example"

def test_format_column_full():
    # Test with regular input
    assert format_column("ExampleName", "AnotherExample", mode="full") == "example_name_another_example"
    # Test with special characters
    assert format_column("Example;Name", "Another,Example", mode="full") == "example_name_another_example"
    # Test with empty string
    assert format_column("", mode="full") != ""
    # Test with multiple underscores
    assert format_column("Example__Name", "Another__Example", mode="full") == "example_name_another_example"
    # Test with PascalCase and CamelCase
    assert format_column("PascalCase", "camelCase", mode="full") == "pascal_case_camel_case"
    # Test with edge cases
    assert format_column("áéíóú", mode="full") == "aeiou"
    # Test with non-alphanumeric characters
    assert format_column("Example@Name#With$Special%Characters^", mode="full") == "example_name_with_special_characters"

def test_format_column_errors():
    # Test with no arguments
    with pytest.raises(ValueError):
        format_column(mode="light")
    with pytest.raises(ValueError):
        format_column(mode="full")
    # Test with too long string
    long_string = "a" * 128
    with pytest.raises(ValueError):
        format_column(long_string, mode="full")
