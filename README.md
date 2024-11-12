## Walmart Sales Data Pipeline with PySpark

### Overview

This project demonstrates a data pipeline for analyzing Walmart sales data using Apache Spark and PySpark in Python. The pipeline loads data from Excel files, processes it with Spark SQL, and visualizes outputs with matplotlib. Data insights are saved as images and files for easy access.

### Prerequisites

Before you begin, ensure you have the following installed:

* **Java Development Kit (JDK):** Apache Spark requires Java to run. Install Java JDK 8 or higher.Download the latest version from Oracle's website.
* **Apache Spark:** Download and install the latest version from the Apache Spark website.
    * Set up Environment Variable
        * Create a `SPARK_HOME` environment variable pointing to your Spark installation directory.
        * Add $SPARK_HOME/bin to your system PATH for easy command-line access.
* **Python:** Install Python 3.x.
* **Required Python Libraries:** Install necessary libraries like `pandas`  and `matplotlib` using pip:

```bash
pip install pandas  matplotlib
```


### Setting up the Spark Environment
* **Start Spark in Command Line:**
  To initialize Spark in local mode, open your command prompt and type:
  ```bash
  pyspark
  ```
This will open an interactive Spark shell where you can directly execute Spark code.

* **Apache Spark Web UI:**
The Spark Web UI helps monitor tasks, stages, and resource allocation. By default, it is accessible at:
* Running locally (http://localhost:4040)
* Docker: [http://host.docker.internal:4040](http://host.docker.internal:4040)

## Code Explanation

### 1\. Importing Required Libraries
```bash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd
 ```
Here, we import essential libraries:
* `SparkSession` and `col` from PySpark for managing data and SQL functions.
* `matplotlib.pyplot` for visualizations.
* `pandas` to handle Excel files easily.

### 2\. Initializing Spark Session with Configuration
```bash
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.ui.port", "4041") \
    .config("spark.num.executors", "2") \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "200")
 ```
This code initializes a SparkSession with specific configurations:
* **AppName:** Names the session for easier identification in the Spark UI.
* **executor.memory and driver memory:** Allocate memory for the driver and executors.
* **executor.cores:** Limits CPU cores used by each executor.
* **ui.port:** Sets the Spark UI to avoid conflicts on the default port 4040.
* **num.executors:** Specifies the number of executors for distributed computing.
* **spark.sql.shuffle.partitions:** Sets partitions to optimize performance during shuffles.

  ### 3\.Loading Data from Excel
```bash
file_path_sales = "C:/Users/user/OneDrive/Desktop/Salestxns.xlsx"
file_path_customer = "C:/Users/user/OneDrive/Desktop/customers.xlsx"
sales_df_pd = pd.read_excel(file_path_sales)
customer_df_pd = pd.read_excel(file_path_customer)
 ```
Using Pandas to load Excel files into DataFrames (`sales_df_pd` and `customer_df_pd`). This is convenient for file handling.
### 4\. Coverting Dataframes to Spark Dataframes
```bash
sales_df = spark.createDataFrame(sales_df_pd)
customer_df = spark.createDataFrame(customer_df_pd)
```
