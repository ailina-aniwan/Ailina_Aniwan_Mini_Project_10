"""
Test cases for PySpark library functions
"""

import os
import pytest
from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    """Fixture to manage Spark session setup and teardown"""
    spark = start_spark("TestApp")
    yield spark
    end_spark(spark)


def test_extract():
    """Test the extract function"""
    file_path = extract()
    assert os.path.exists(file_path), "Extract function did not create the file."


def test_load_data(spark):
    """Test the load_data function"""
    df = load_data(spark)
    assert df is not None, "Load data returned None."
    assert df.count() > 0, "Loaded DataFrame is empty."
    assert "GPA" in df.columns, "GPA column is missing in the DataFrame."


def test_describe(spark):
    """Test the describe function"""
    df = load_data(spark)
    result = describe(df)
    assert result is None, "Describe function should return None (only logs output)."


def test_query(spark):
    """Test the query function"""
    df = load_data(spark)
    query_string = "SELECT * FROM StudentLifestyle WHERE GPA > 3.5"
    result = query(spark, df, query_string, "StudentLifestyle")
    assert result is None, "Query function should return None (only logs output)."


def test_example_transform(spark):
    """Test the example_transform function"""
    df = load_data(spark)
    result = example_transform(df)
    assert result is None, "Transform function should return None (only logs output)."


if __name__ == "__main__":
    # Run tests manually
    file_path = extract()
    assert os.path.exists(file_path), "Extract function did not create the file."

    with start_spark("TestApp") as spark:
        df = load_data(spark)
        assert df is not None, "Load data returned None."
        assert df.count() > 0, "Loaded DataFrame is empty."
        describe(df)
        query_string = "SELECT * FROM StudentLifestyle WHERE GPA > 3.5"
        query(spark, df, query_string, "StudentLifestyle")
        example_transform(df)
