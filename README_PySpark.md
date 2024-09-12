# Highest Monthly Sales Calculation with PySpark
This script processes sales order data from a CSV file, calculates the highest sales per month, and uses a window function to identify the top sale for each month.

# Requirements
Apache Spark with PySpark installed.
A CSV file containing sales order details, located at /FileStore/tables/Sales_SalesOrderDetail__2_.csv.

# Schema Definition
The schema defines the structure of the sales order data using PySpark's StructType and StructField. The important columns include SalesOrderID, OrderQty, UnitPrice, LineTotal, and ModifiedDate.

schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    ...
    StructField("ModifiedDate", DateType(), True)
])

# Code Overview
**Load Data**: The script reads the sales order data from a CSV file using the defined schema.

sales_order_detail_df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail__2_.csv")

**Extract Year and Month:** New columns, OrderYear and OrderMonth, are created by extracting the year and month from the ModifiedDate column.

sales_order_detail_df = sales_order_detail_df.withColumn("OrderYear", F.year("ModifiedDate")) \
                                             .withColumn("OrderMonth", F.month("ModifiedDate"))

**Calculate Total Sales per Month:** The total sales for each month are calculated by grouping the data by OrderYear and OrderMonth and finding the highest sale using max("LineTotal").

monthly_sales_df = sales_order_detail_df.groupBy("OrderYear", "OrderMonth").agg(F.max("LineTotal").alias("HighestSale"))

**Identify the Highest Sale per Month**: A window function is applied to rank the highest sale per month. The top-ranked sale is filtered for each month.

window_spec = Window.partitionBy("OrderYear", "OrderMonth").orderBy(F.desc("HighestSale"))
highest_monthly_sales_df = monthly_sales_df.withColumn("Rank", F.rank().over(window_spec)).filter(F.col("Rank") == 1).drop("Rank")

**Display the Result:** The highest sales for each month are displayed, ordered by year and month.

highest_monthly_sales_df.select("OrderYear", "OrderMonth", "HighestSale").orderBy("OrderYear", "OrderMonth").show()

**Example Output**
The result is a list of the highest sales for each month, along with the corresponding year and month.

# Usage Instructions
Ensure PySpark is installed and configured in your environment.
Update the file path to the CSV if needed.
Run the script to load the sales data, calculate the highest sales per month, and display the results.
