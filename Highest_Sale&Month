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

# Extract Year and Month from ModifiedDate
sales_order_detail_df = sales_order_detail_df.withColumn("OrderYear", F.year("ModifiedDate")) \
                                             .withColumn("OrderMonth", F.month("ModifiedDate"))

# Calculate total sale per SalesOrderID per month
monthly_sales_df = sales_order_detail_df.groupBy(
    "OrderYear",
    "OrderMonth",
).agg(
    F.max("LineTotal").alias("HighestSale")
)

# Find the highest sale each month using a window function
window_spec = Window.partitionBy("OrderYear", "OrderMonth").orderBy(F.desc("HighestSale"))

highest_monthly_sales_df = monthly_sales_df.withColumn(
    "Rank", F.rank().over(window_spec)
).filter(F.col("Rank") == 1).drop("Rank")

# Display the result
highest_monthly_sales_df.select("OrderYear", "OrderMonth", "HighestSale").orderBy("OrderYear", "OrderMonth").show()
