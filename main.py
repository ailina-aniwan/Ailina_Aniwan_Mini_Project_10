"""
Main CLI or app entry point
"""

from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


def main():

    extract()

    spark = start_spark("StudentLifestyleAnalysis")
    df = load_data(spark)

    describe(df)
    df = example_transform(df)

    query(
        spark,
        df,
        """
        SELECT Stress_Level, AVG(GPA_Rounded) AS Avg_Rounded_GPA
        FROM StudentLifestyle
        GROUP BY Stress_Level
        ORDER BY Avg_Rounded_GPA DESC
        """,
        "StudentLifestyle",
    )

    end_spark(spark)


if __name__ == "__main__":
    main()
