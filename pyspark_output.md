The operation is load data

The truncated output is: 

|    |   Student_ID |   Study_Hours_Per_Day |   Sleep_Hours_Per_Day |   Social_Hours_Per_Day |   Physical_Activity_Hours_Per_Day |   GPA | Stress_Level   |
|---:|-------------:|----------------------:|----------------------:|-----------------------:|----------------------------------:|------:|:---------------|
|  0 |            1 |                   6.9 |                   8.7 |                    2.8 |                               1.8 |  2.99 | Moderate       |
|  1 |            2 |                   5.3 |                   8   |                    4.2 |                               3   |  2.75 | Low            |
|  2 |            3 |                   5.1 |                   9.2 |                    1.2 |                               4.6 |  2.67 | Low            |
|  3 |            4 |                   6.5 |                   7.2 |                    1.7 |                               6.5 |  2.88 | Moderate       |
|  4 |            5 |                   8.1 |                   6.5 |                    2.2 |                               6.6 |  3.51 | High           |
|  5 |            6 |                   6   |                   8   |                    0.3 |                               7.6 |  2.85 | Moderate       |
|  6 |            7 |                   8   |                   5.3 |                    5.7 |                               4.3 |  3.08 | High           |
|  7 |            8 |                   8.4 |                   5.6 |                    3   |                               5.2 |  3.2  | High           |
|  8 |            9 |                   5.2 |                   6.3 |                    4   |                               4.9 |  2.82 | Low            |
|  9 |           10 |                   7.7 |                   9.8 |                    4.5 |                               1.3 |  2.76 | Moderate       |

The operation is describe data

The truncated output is: 

|    | summary   |   Student_ID |   Study_Hours_Per_Day |   Sleep_Hours_Per_Day |   Social_Hours_Per_Day |   Physical_Activity_Hours_Per_Day |         GPA | Stress_Level   |
|---:|:----------|-------------:|----------------------:|----------------------:|-----------------------:|----------------------------------:|------------:|:---------------|
|  0 | count     |     1000     |             1000      |            1000       |             1000       |                         1000      | 1000        | 1000           |
|  1 | mean      |      500.5   |                7.4613 |               7.5115  |                2.6718  |                            4.3313 |    3.11736  |                |
|  2 | stddev    |      288.819 |                1.4572 |               1.45107 |                1.69696 |                            2.5804 |    0.303875 |                |
|  3 | min       |        1     |                5      |               5       |                0       |                            0      |    2.24     | High           |
|  4 | max       |     1000     |               10      |              10       |                6       |                           13      |    4        | Moderate       |

The operation is transform data

The truncated output is: 

|    |   Student_ID |   Study_Hours_Per_Day |   Sleep_Hours_Per_Day |   Social_Hours_Per_Day |   Physical_Activity_Hours_Per_Day |   GPA | Stress_Level   | Lifestyle_Category    |   GPA_Rounded |
|---:|-------------:|----------------------:|----------------------:|-----------------------:|----------------------------------:|------:|:---------------|:----------------------|--------------:|
|  0 |            1 |                   6.9 |                   8.7 |                    2.8 |                               1.8 |  2.99 | Moderate       | Balanced              |           3   |
|  1 |            2 |                   5.3 |                   8   |                    4.2 |                               3   |  2.75 | Low            | Balanced              |           2.8 |
|  2 |            3 |                   5.1 |                   9.2 |                    1.2 |                               4.6 |  2.67 | Low            | Balanced              |           2.7 |
|  3 |            4 |                   6.5 |                   7.2 |                    1.7 |                               6.5 |  2.88 | Moderate       | Active Lifestyle      |           2.9 |
|  4 |            5 |                   8.1 |                   6.5 |                    2.2 |                               6.6 |  3.51 | High           | High Study Commitment |           3.5 |
|  5 |            6 |                   6   |                   8   |                    0.3 |                               7.6 |  2.85 | Moderate       | Active Lifestyle      |           2.8 |
|  6 |            7 |                   8   |                   5.3 |                    5.7 |                               4.3 |  3.08 | High           | High Study Commitment |           3.1 |
|  7 |            8 |                   8.4 |                   5.6 |                    3   |                               5.2 |  3.2  | High           | High Study Commitment |           3.2 |
|  8 |            9 |                   5.2 |                   6.3 |                    4   |                               4.9 |  2.82 | Low            | Balanced              |           2.8 |
|  9 |           10 |                   7.7 |                   9.8 |                    4.5 |                               1.3 |  2.76 | Moderate       | High Study Commitment |           2.8 |

The operation is query data

The query is 
        SELECT Stress_Level, AVG(GPA_Rounded) AS Avg_Rounded_GPA
        FROM StudentLifestyle
        GROUP BY Stress_Level
        ORDER BY Avg_Rounded_GPA DESC
        

The truncated output is: 

|    | Stress_Level   |   Avg_Rounded_GPA |
|---:|:---------------|------------------:|
|  0 | High           |           3.26388 |
|  1 | Moderate       |           3.03548 |
|  2 | Low            |           2.80976 |

