## Walmart Sales Data Pipeline with PySpark

## Overview

This project demonstrates a data pipeline for analyzing Walmart sales data using Apache Spark and PySpark in Python. The pipeline loads data from Excel files, processes it with Spark SQL, and visualizes outputs with matplotlib. Data insights are saved as images and files for easy access.

## Prerequisites of Installing Apache Spark

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

## Why use Apache Spark
Apache Spark is a powerful open-source framework used for large-scale data processing and analysis. Key reasons for using Spark in this project include:

### 1\. High-Performance Computation:
 * Spark provides in-memory computation, which makes it much faster for iterative tasks compared to traditional MapReduce on Hadoop. This is especially useful for complex data transformations and analytical queries on large datasets.

### 2\. Distributed Processing:
* Spark distributes data across a cluster of machines and performs operations in parallel. This setup reduces processing time significantly compared to handling data on a single machine, making it ideal for large datasets like sales transaction data.

### 3\. Versatile APIs and Language Support:
* Spark’s DataFrame and SQL APIs make it convenient to work with structured data. It supports languages like Python, Scala, and Java, making it accessible to a wide range of developers.

### 4\. Built-in Libraries:

* Spark includes libraries for SQL, streaming, machine learning, and graph processing, which allows for flexible and comprehensive data processing workflows.
  
## Setting up the Spark Environment
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
We convert Pandas DataFrames to Spark DataFrames for Spark processing. Spark DataFrames enable distributed processing and efficient querying

### 5\.Caching DataFrames

```bash
sales_df.cache()
customer_df.cache()
```
### 6\.Registering Temporary views

```bash
sales_df.createOrReplaceTempView("sales_data")
customer_df.createOrReplaceTempView("customers")
```
Temporary views allow running SQL queries directly on Spark DataFrames.

### 7\.SQL Queries and Visualization
**1. Total number of unique customers:**

```bash
total_customers = spark.sql("SELECT COUNT(DISTINCT Customer_Id) AS Total_Customers FROM customers")
total_customers_pd = total_customers.toPandas()
print("Total Number of Unique Customers:", total_customers_pd['Total_Customers'][0])
```
This query finds the total number of unique customers by counting distinct Customer_Id values.

**2. Total Sales by State:**

```bash
sales_by_state = spark.sql("""
    SELECT c.State, SUM(s.Price * s.Quantity) AS Total_Sales
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    GROUP BY c.State
""")
sales_by_state_pd = sales_by_state.toPandas()
```
This query aggregates sales per state and visualizes the data with a pie chart if there are fewer than 10 states; otherwise, it uses a bar chart.

**3. Top 10 Most Purchased Products:**

```bash
top_products = spark.sql("""
    SELECT Product_Name, SUM(Quantity) AS Total_Quantity
    FROM sales_data
    GROUP BY Product_Name
    ORDER BY Total_Quantity DESC
    LIMIT 10
""")
top_products_pd = top_products.toPandas()
```
Aggregates sales quantity by product name, displays the top 10 most purchased products.

**4. Average Transaction Value:**

```bash
avg_transaction_value = spark.sql("SELECT AVG(Price * Quantity) AS Avg_Transaction_Value FROM sales_data")
avg_transaction_value_pd = avg_transaction_value.toPandas()
print("Average Transaction Value:", avg_transaction_value_pd['Avg_Transaction_Value'][0])
```
Calculates the average transaction value by multiplying `Price` and `Quantity`.

**5. Top 5 Customers by Expenditure:**

```bash
top_customers = spark.sql("""
    SELECT c.Customer_Id, c.Name, SUM(s.Price * s.Quantity) AS Total_Spent
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    GROUP BY c.Customer_Id, c.Name
    ORDER BY Total_Spent DESC
    LIMIT 5
""")
top_customers_pd = top_customers.toPandas()
```
Shows the top 5 customers by expenditure using a horizontal bar chart.

**6. Product Purchases by Specific Customer:**

```bash
customer_purchases = spark.sql("""
    SELECT s.Product_Name, SUM(s.Quantity * s.Price) AS Total_Amount
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    WHERE c.Customer_Id = 245
    GROUP BY s.Product_Name
""")
customer_purchases_pd = customer_purchases.toPandas()
```
Analyzes product purchases for a specific customer (Customer ID 245). Plots data using a pie chart if there are fewer than 10 products; otherwise, uses a bar chart.

**7. Monthly Sales Trends:**
To analyze monthly sales trends and identify the month with the highest sales, we’ll assume there is a date field in the sales_data table. In this example, let’s call the field `Transaction_Date`. Here’s the SQL query to calculate total monthly sales and identify the month with the highest sales.

```bash
# SQL query to calculate monthly sales and identify the highest sales month
monthly_sales_trend = spark.sql("""
    SELECT 
        MONTH(Transaction_Date) AS Month,
        YEAR(Transaction_Date) AS Year,
        SUM(Price * Quantity) AS Total_Sales
    FROM sales_data
    GROUP BY YEAR(Transaction_Date), MONTH(Transaction_Date)
    ORDER BY Year, Month
""")
```
This query will output each month’s sales and indicate which month had the highest sales in a given year.

**8. Category with Highest Sales:**

```bash
top_category = spark.sql("""
    SELECT Category_Name, SUM(Price * Quantity) AS Total_Category_Sales
    FROM sales_data
    GROUP BY Category_Name
    ORDER BY Total_Category_Sales DESC
    LIMIT 1
""")
top_category_pd = top_category.toPandas()
print("Category with Highest Sales:", top_category_pd['Category_Name'][0])
```
This query identifies the category with the highest sales.

**9. State-Wise Sales Comparison (Texas vs Ohio):**

```bash
state_comparison = spark.sql("""
    SELECT c.State, SUM(s.Price * Quantity) AS Total_State_Sales
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    WHERE c.State IN ('TX', 'OH')
    GROUP BY c.State
""")
state_comparison_pd = state_comparison.toPandas()
```
Compares sales between Texas and Ohio using a pie chart.

**10. Detailed Customer Purchase Report:**

```bash
customer_report = spark.sql("""
    SELECT DISTINCT (c.Customer_Id), c.Name, 
           SUM(s.Price * Quantity) AS Total_Purchases, 
           COUNT(*) AS Total_Transactions, 
           AVG(s.Price * Quantity) AS Avg_Transaction_Value
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    GROUP BY c.Customer_Id, c.Name ORDER BY Customer_Id
""")
customer_report_pd = customer_report.toPandas()
customer_report_pd.to_csv("customer_report.csv", index=False)
```
Generates a detailed report of each customer's purchases, transactions, and average transaction value, saved as `customer_report.csv`.


