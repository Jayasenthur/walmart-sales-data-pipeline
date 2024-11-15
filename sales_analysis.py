
```python
# Import necessary libraries
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

# Initialize Spark Session with configuration
# Configures 2 executors to handle tasks in parallel.
# Allocates 2 CPU cores to each executor.
# Sets the UI port to 4041, where we can monitor Spark job progress.
try:    
    spark = SparkSession.builder \
        .appName("Sales Analysis") \
        .config("spark.executor.memory", "4g") \ # Allocates 2 CPU cores to each executor.
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.ui.port", "4041") \
        .config("spark.num.executors", "2") \
        .getOrCreate()

    # Sets the number of shuffle partitions to 200, optimizing performance during joins and aggregations by reducing shuffle tasks.
    spark.conf.set("spark.sql.shuffle.partitions", "200")

# Handles any errors during the Spark session setup, printing an error message and exiting if an error occurs.
except Exception as e:
    print(f"Error starting SparkSession: {str(e)}")
    exit(1)

# Load Excel data
file_path_sales = "C:/Users/user/OneDrive/Desktop/Salestxns.xlsx"
file_path_customer = "C:/Users/user/OneDrive/Desktop/customers.xlsx"

# Reads an Excel file into a Pandas DataFrame for salestxns data.
sales_df_pd = pd.read_excel(file_path_sales)

# Reads the customer Excel file into a Pandas DataFrame.
customer_df_pd = pd.read_excel(file_path_customer)

# Convert to Spark DataFrames
sales_df = spark.createDataFrame(sales_df_pd)
customer_df = spark.createDataFrame(customer_df_pd)

# Cache DataFrames in memory to improve repeated access performance by avoiding re-computation.
sales_df.cache()
customer_df.cache()

# Register the DataFrames as temporary SQL views that allows us to use SQL commands to query our data like a database.
sales_df.createOrReplaceTempView("sales_data")
customer_df.createOrReplaceTempView("customers")

# SQL Analysis and Conditional Plotting

# 1. SQL query counts unique customers in the "customers" view.
total_customers = spark.sql("SELECT COUNT(DISTINCT Customer_Id) AS Total_Customers FROM customers")
print(total_customers.show())

# 2. SQL Query joins sales and customers to get total sales per state and groups results by state..
sales_by_state = spark.sql("""
    SELECT c.State, SUM(s.Price * s.Quantity) AS Total_Sales
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    GROUP BY c.State
""")
sales_by_state_pd = sales_by_state.toPandas()

# Check if there are enough unique states to plot a pie chart, otherwise use a bar chart
if len(sales_by_state_pd) <= 10:
    plt.figure()
    plt.pie(sales_by_state_pd['Total_Sales'], labels=sales_by_state_pd['State'], autopct='%1.1f%%')
    plt.title('Total Sales by State')
    plt.savefig('sales_by_state_pie.png')
else:
    plt.figure()
    plt.bar(sales_by_state_pd['State'], sales_by_state_pd['Total_Sales'])
    plt.xlabel('State')
    plt.ylabel('Total Sales')
    plt.title('Total Sales by State')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Saves the chart as an image file.
    plt.savefig('sales_by_state_bar.png')

# 3. SQL Query aggregates sales quantities by product name and selects the top 10.
top_products = spark.sql("""
    SELECT Product_Name, SUM(Quantity) AS Total_Quantity
    FROM sales_data
    GROUP BY Product_Name
    ORDER BY Total_Quantity DESC
    LIMIT 10
""")
top_products_pd = top_products.toPandas()
plt.figure()
plt.bar(top_products_pd['Product_Name'], top_products_pd['Total_Quantity'])
plt.xlabel('Product Name')
plt.ylabel('Quantity Sold')
plt.title('Top 10 Most Purchased Products')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('top_products.png')

# 4. Average Transaction Value (Single Output, No Plot Needed)
avg_transaction_value = spark.sql("SELECT AVG(Price * Quantity) AS Avg_Transaction_Value FROM sales_data")
avg_transaction_value_pd = avg_transaction_value.toPandas()
print("Average Transaction Value:", avg_transaction_value_pd['Avg_Transaction_Value'][0])

# 5. Top 5 Customers by Expenditure (Horizontal Bar for better readability)
top_customers = spark.sql("""
    SELECT c.Customer_Id, c.Name, SUM(s.Price * s.Quantity) AS Total_Spent
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    GROUP BY c.Customer_Id, c.Name
    ORDER BY Total_Spent DESC
    LIMIT 5
""")
top_customers_pd = top_customers.toPandas()
plt.figure()
plt.barh(top_customers_pd['Name'], top_customers_pd['Total_Spent'], color='skyblue')
plt.xlabel('Total Expenditure')
plt.ylabel('Customer Name')
plt.title('Top 5 Customers by Expenditure')
plt.tight_layout()
plt.savefig('top_customers.png')

# 6. Product Purchases by a Specific Customer (ID 245) (Pie Chart if product count is small)
customer_purchases = spark.sql("""
    SELECT s.Product_Name, SUM(s.Quantity * s.Price) AS Total_Amount
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    WHERE c.Customer_Id = 245
    GROUP BY s.Product_Name
""")
customer_purchases_pd = customer_purchases.toPandas()

if len(customer_purchases_pd) <= 10:
    plt.figure()
    plt.pie(customer_purchases_pd['Total_Amount'], labels=customer_purchases_pd['Product_Name'], autopct='%1.1f%%')
    plt.title('Product Purchases by Customer ID 245')
    plt.savefig('customer_purchases_245_pie.png')
else:
    plt.figure()
    plt.bar(customer_purchases_pd['Product_Name'], customer_purchases_pd['Total_Amount'])
    plt.xlabel('Product Name')
    plt.ylabel('Total Amount Spent')
    plt.title('Product Purchases by Customer ID 245')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('customer_purchases_245_bar.png')

# 7. Monthly Sales Trends

# SQL query to calculate monthly sales and identify the highest sales month
#monthly_sales_trend = spark.sql("""
    #SELECT 
     #   MONTH(Transaction_Date) AS Month,
      #  YEAR(Transaction_Date) AS Year,
       # SUM(Price * Quantity) AS Total_Sales
    #FROM sales_data
    #GROUP BY YEAR(Transaction_Date), MONTH(Transaction_Date)
    #ORDER BY Year, Month
#""")

# 8. Category with Highest Sales (No Plot for Single Category Result)
top_category = spark.sql("""
    SELECT Category_Name, SUM(Price * Quantity) AS Total_Category_Sales
    FROM sales_data
    GROUP BY Category_Name
    ORDER BY Total_Category_Sales DESC
    LIMIT 1
""")
top_category_pd = top_category.toPandas()
print("Category with Highest Sales:", top_category_pd['Category_Name'][0])

# 9. State-wise Sales Comparison (Texas vs. Ohio, Pie Chart if only 2 states)
state_comparison = spark.sql("""
    SELECT c.State, SUM(s.Price * Quantity) AS Total_State_Sales
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    WHERE c.State IN ('TX', 'OH')
    GROUP BY c.State
""")
state_comparison_pd = state_comparison.toPandas()

plt.figure()
plt.pie(state_comparison_pd['Total_State_Sales'], labels=state_comparison_pd['State'], autopct='%1.1f%%')
plt.title('Sales Comparison: Texas vs. Ohio')
plt.savefig('state_comparison_pie.png')

# 10. Detailed Customer Purchase Report (Save as CSV for Report Integration)
customer_report = spark.sql("""
    SELECT DISTINCT (c.Customer_Id), c.Name,
        SUM(s.Price * s.Quantity) AS Total_Purchases,
        COUNT(*) AS Total_Transactions,
        AVG(s.Price * Quantity) AS Avg_Transaction_Value
    FROM sales_data s
    JOIN customers c ON s.Customer_Id = c.Customer_Id
    GROUP BY c.Customer_Id, c.Name
    ORDER BY Customer_Id
""")

customer_report_pd = customer_report.toPandas()
customer_report_pd.to_csv("customer_report.csv", index=False)

# Stop the SparkSession
spark.stop()
```
