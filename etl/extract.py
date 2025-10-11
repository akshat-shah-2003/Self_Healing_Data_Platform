import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

query = "SELECT ORDERNUMBER AS ORDER_ID, QUANTITYORDERED AS ORDER_QUANTITY, PRICEEACH AS PRICE," \
"ORDERLINENUMBER AS LINE_ITEM, SALES, TO_DATE(ORDERDATE,'MM/DD/YYYY 0:00') AS ORDER_DATE, STATUS AS ORDER_STATUS," \
"QTR_ID AS ORDER_QUARTER, MONTH_ID AS ORDER_MONTH, YEAR_ID ORDER_YEAR, PRODUCTLINE AS PRODUCT_LINE, MSRP, PRODUCTCODE AS PRODUCT_ID," \
"CUSTOMERNAME AS CUSTOMER_NAME, PHONE, ADDRESSLINE1 AS ADDRRESS, CITY, COUNTRY," \
"CONTACTLASTNAME AS LAST_NAME, CONTACTFIRSTNAME AS FIRST_NAME, DEALSIZE AS DEAL_SIZE FROM SALES_DATA;"

df = pd.read_sql(query, conn)

df.to_csv("./data/raw/sales_data.csv", index=False)
print("Raw data extracted and saved successfully.")

conn.close()
