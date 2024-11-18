"""
Test cases for PySpark library functions
"""

import os
import pytest
from unittest.mock import patch
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
    """Fixture to initialize and teardown Spark session"""
    spark_session = start_spark("TestApp")
    yield spark_session
    end_spark(spark_session)


def test_extract():
    """Test the extract function"""
    file_path = extract()
    assert os.path.exists(file_path), "Extract function did not create the file."


@patch("mylib.lib.log_output")
def test_load_data(mock_log, spark):
    """Test the load_data function"""
    df = load_data(spark)
    assert df is not None, "Load data returned None."
    assert df.count() > 0, "Loaded DataFrame is empty."
    assert "GPA" in df.columns, "GPA column is missing in the DataFrame."


@patch("mylib.lib.log_output")
def test_describe(mock_log, spark):
    """Test the describe function"""
    df = load_data(spark)
    result = describe(df)
    assert result is None, "Describe function should return None (only logs output)."


@patch("mylib.lib.log_output")
def test_query(mock_log, spark):
    """Test the query function"""
    df = load_data(spark)
    df = example_transform(df)
    query_string = """
        SELECT Stress_Level, AVG(GPA_Rounded) AS Avg_Rounded_GPA
        FROM StudentLifestyle
        GROUP BY Stress_Level
        ORDER BY Avg_Rounded_GPA DESC
    """
    result = query(spark, df, query_string, "StudentLifestyle")
    assert result is None, "Query function should return None (only logs output)."


@patch("mylib.lib.log_output")
def test_example_transform(mock_log, spark):
    """Test the example_transform function"""
    df = load_data(spark)
    transformed_df = example_transform(df)
    assert transformed_df is not None, "Transform function returned None."
    assert (
        "Lifestyle_Category" in transformed_df.columns
    ), "Lifestyle_Category column is missing."
    assert "GPA_Rounded" in transformed_df.columns, "GPA_Rounded column is missing."


if __name__ == "__main__":
    with patch("mylib.lib.log_output"):
        file_path = extract()
        assert os.path.exists(file_path), "Extract function did not create the file."

        spark_session = start_spark("TestApp")
        df = load_data(spark_session)
        assert df is not None, "Load data returned None."
        assert df.count() > 0, "Loaded DataFrame is empty."
        describe(df)
        df = example_transform(df)
        query_string = """
            SELECT Stress_Level, AVG(GPA_Rounded) AS Avg_Rounded_GPA
            FROM StudentLifestyle
            GROUP BY Stress_Level
            ORDER BY Avg_Rounded_GPA DESC
        """
        query(spark_session, df, query_string, "StudentLifestyle")
