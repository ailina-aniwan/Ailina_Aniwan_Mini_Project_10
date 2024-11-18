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
    df = example_transform(df)  # Ensure transformations are applied before querying
    query_string = """
        SELECT Stress_Level, AVG(GPA_Rounded) AS Avg_Rounded_GPA
        FROM StudentLifestyle
        GROUP BY Stress_Level
        ORDER BY Avg_Rounded_GPA DESC
    """
    result = query(spark, df, query_string, "StudentLifestyle")
    assert result is None, "Query function should return None (only logs output)."


def test_example_transform(spark):
    """Test the example_transform function"""
    df = load_data(spark)
    transformed_df = example_transform(df)
    assert transformed_df is not None, "Transform function returned None."
    assert "Lifestyle_Category" in transformed_df.columns, "Lifestyle_Category column is missing."
    assert "GPA_Rounded" in transformed_df.columns, "GPA_Rounded column is missing."


if __name__ == "__main__":
    # Run tests manually
    file_path = extract()
    assert os.path.exists(file_path), "Extract function did not create the file."

    with start_spark("TestApp") as spark:
        df = load_data(spark)
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
        query(spark, df, query_string, "StudentLifestyle")
