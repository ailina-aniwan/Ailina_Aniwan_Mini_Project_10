"""
Library functions for PySpark processing
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, round
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)

LOG_FILE = "pyspark_output.md"


def log_output(operation, output, query=None):
    """Adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def end_spark(spark):
    spark.stop()
    return "Stopped Spark session"


def extract(
    url="https://github.com/ailina-aniwan/Ailina_Aniwan_Mini_Project_10/raw/refs/heads/main/data/student_lifestyle_dataset.csv",
    file_path="data/student_lifestyle_dataset.csv",
    directory="data",
):
    """Extract a URL to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    return file_path


def load_data(spark, data="data/student_lifestyle_dataset.csv", name="StudentLifestyle"):
    """Load data with a predefined schema"""
    schema = StructType([
        StructField("Student_ID", IntegerType(), True),
        StructField("Study_Hours_Per_Day", FloatType(), True),
        StructField("Sleep_Hours_Per_Day", FloatType(), True),
        StructField("Social_Hours_Per_Day", FloatType(), True),
        StructField("Physical_Activity_Hours_Per_Day", FloatType(), True),
        StructField("GPA", FloatType(), True),
        StructField("Stress_Level", StringType(), True),
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query, name):
    """Queries using Spark SQL"""
    df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()


def describe(df):
    """Generate summary statistics"""
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()


def example_transform(df):
    """Example transformation: Categorize lifestyle"""
    df = df.withColumn(
        "Lifestyle_Category",
        when(col("Study_Hours_Per_Day") > 7, "High Study Commitment")
        .when(col("Physical_Activity_Hours_Per_Day") > 5, "Active Lifestyle")
        .otherwise("Balanced"),
    ).withColumn("GPA_Rounded", round(col("GPA"), 1))

    log_output("transform data", df.limit(10).toPandas().to_markdown())

    return df  # Ensure the transformed DataFrame is returned
