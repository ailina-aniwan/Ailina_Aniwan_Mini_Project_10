[![CI](https://github.com/ailina-aniwan/Ailina_Aniwan_Mini_Project_10/actions/workflows/cicd.yml/badge.svg)](https://github.com/ailina-aniwan/Ailina_Aniwan_Mini_Project_10/actions/workflows/cicd.yml)

# IDS706 - Mini Project 10 - Ailina Aniwan

## PySpark Data Processing

### ✔️ Project Overview
This project leverages PySpark to process and analyze a dataset of student lifestyle data. The goal is to demonstrate distributed data processing through transformations and SQL queries, with results logged in a markdown file (`pyspark_output.md`).

### ✔️ Project Requirements
- Perform data processing on a large dataset using PySpark.
- Implement at least one Spark SQL query and one data transformation.

### ✔️ Core Functionality
- Data Loading: Load the dataset into a PySpark DataFrame using the `load_data` function.
- Data Transformation: Categorize students' lifestyles and round GPA values with the `example_transform` function.
- SQL Query Execution: Use a Spark SQL query to compute average rounded GPA by stress levels using the `query` function.
- Summary Statistics: Generate descriptive statistics with the `describe` function.
- Logging: Log results and operations to `pyspark_output.md` for easy review.

### ✔️ Reference
- https://github.com/nogibjj/python-ruff-template