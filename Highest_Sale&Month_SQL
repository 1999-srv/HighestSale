from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql import functions as F
from pyspark.sql import Window

# Define the schema
schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("SpecialOfferID", IntegerType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("UnitPriceDiscount", DecimalType(10, 2), True),
    StructField("LineTotal", DecimalType(20, 2), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", DateType(), True)
])

# Load the data with the schema
sales_order_detail_df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail__2_.csv")
sales_order_detail_df.createOrReplaceTempView("sales_order_detail")

%sql
-- SQL Query to extract year and month from ModifiedDate
CREATE OR REPLACE TEMP VIEW sales_order_with_date AS
SELECT *,
       YEAR(ModifiedDate) AS OrderYear,
       MONTH(ModifiedDate) AS OrderMonth
FROM sales_order_detail;

-- SQL Query to calculate total sale per SalesOrderID per month
CREATE OR REPLACE TEMP VIEW monthly_sales AS
SELECT OrderYear, 
       OrderMonth, 
       MAX(LineTotal) AS HighestSale
FROM sales_order_with_date
GROUP BY OrderYear, OrderMonth;

-- SQL Query to find the highest sale for each month using window function
WITH ranked_sales AS (
    SELECT *,
           RANK() OVER (PARTITION BY OrderYear, OrderMonth ORDER BY HighestSale DESC) AS Rank
    FROM monthly_sales
)
-- Filter to get the highest sale for each month
SELECT OrderYear, OrderMonth, HighestSale
FROM ranked_sales
WHERE Rank = 1
ORDER BY OrderYear, OrderMonth;
