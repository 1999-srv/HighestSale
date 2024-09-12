# Highest Monthly Sales Calculation with SQL
This set of SQL queries extracts, processes, and ranks sales data to determine the highest sales per month from the sales_order_detail table.
**Overview**
This SQL script performs the following steps:
Extracts the year and month from the ModifiedDate column.
Groups the data by year and month to calculate the highest sales (LineTotal) per month.
Uses a window function to rank and identify the top sale for each month.
Displays the highest sale for each year and month, ordered chronologically.

**Step 1: Extract Year and Month** The first query extracts the year and month from the ModifiedDate column and creates a temporary view, sales_order_with_date.

       CREATE OR REPLACE TEMP VIEW sales_order_with_date AS
       SELECT *,
              YEAR(ModifiedDate) AS OrderYear,
              MONTH(ModifiedDate) AS OrderMonth
       FROM sales_order_detail;

**Step 2: Calculate Total Sales per Month** This query calculates the highest LineTotal (sale amount) for each year and month, grouping the data by OrderYear and OrderMonth. The result is stored in a temporary view monthly_sales.

       CREATE OR REPLACE TEMP VIEW monthly_sales AS
       SELECT OrderYear, 
              OrderMonth, 
              MAX(LineTotal) AS HighestSale
       FROM sales_order_with_date
       GROUP BY OrderYear, OrderMonth;

**Step 3: Rank Sales Using a Window Function** The query ranks the sales for each month using the SQL RANK() window function, assigning a rank based on the highest sale (HighestSale). The result is stored as a common table expression (CTE) ranked_sales.
       
       WITH ranked_sales AS (
           SELECT *,
                  RANK() OVER (PARTITION BY OrderYear, OrderMonth ORDER BY HighestSale DESC) AS Rank
           FROM monthly_sales
       )

**Step 4: Filter and Display the Highest Sales**The final query filters the data to retrieve only the highest-ranked sale for each month (Rank = 1), and orders the output by year and month.

       SELECT OrderYear, OrderMonth, HighestSale
       FROM ranked_sales
       WHERE Rank = 1
       ORDER BY OrderYear, OrderMonth;

# Usage Instructions
Ensure the sales_order_detail table is available in your SQL environment.
Execute each SQL query in sequence to calculate and retrieve the highest sales for each month.
The results will show the highest sale amounts per month, ordered by year and month.

# Example Output
The final output contains the following columns:
OrderYear: The year of the sales order.
OrderMonth: The month of the sales order.
HighestSale: The highest sale amount (LineTotal) for that month.
