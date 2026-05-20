# Databricks notebook source
bronze_df = spark.read.parquet("/Volumes/stock_pipeline/bronze/stock_data/*/*/*.parquet")

# COMMAND ----------

# Show the shape and a sample
print(f"Total rows: {bronze_df.count()}")
print(f"Columns: {bronze_df.columns}")
bronze_df.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CAST(Date AS DATE)                    AS Date,
# MAGIC     Ticker,
# MAGIC     ROUND(Open, 2)                        AS Open,
# MAGIC     ROUND(High, 2)                        AS High,
# MAGIC     ROUND(Low, 2)                         AS Low,
# MAGIC     ROUND(Close, 2)                       AS Close,
# MAGIC     CAST(Volume AS INT)                   AS Volume,
# MAGIC     CASE Ticker
# MAGIC         WHEN 'META'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'AAPL'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'AMZN'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'NFLX'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'GOOGL' THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'MSFT'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'NVDA'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'AMD'   THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'INTC'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'TSLA'  THEN 'FAANG & Big Tech'
# MAGIC         WHEN 'JPM'   THEN 'Finance'
# MAGIC         WHEN 'GS'    THEN 'Finance'
# MAGIC         WHEN 'BAC'   THEN 'Finance'
# MAGIC         WHEN 'MS'    THEN 'Finance'
# MAGIC         WHEN 'V'     THEN 'Finance'
# MAGIC         WHEN 'MA'    THEN 'Finance'
# MAGIC         WHEN 'AXP'   THEN 'Finance'
# MAGIC         WHEN 'BLK'   THEN 'Finance'
# MAGIC         WHEN 'JNJ'   THEN 'Healthcare'
# MAGIC         WHEN 'PFE'   THEN 'Healthcare'
# MAGIC         WHEN 'UNH'   THEN 'Healthcare'
# MAGIC         WHEN 'ABBV'  THEN 'Healthcare'
# MAGIC         WHEN 'MRK'   THEN 'Healthcare'
# MAGIC         WHEN 'WMT'   THEN 'Retail'
# MAGIC         WHEN 'TGT'   THEN 'Retail'
# MAGIC         WHEN 'NKE'   THEN 'Retail'
# MAGIC         WHEN 'MCD'   THEN 'Retail'
# MAGIC         WHEN 'SBUX'  THEN 'Retail'
# MAGIC         WHEN 'COST'  THEN 'Retail'
# MAGIC         WHEN 'XOM'   THEN 'Energy'
# MAGIC         WHEN 'CVX'   THEN 'Energy'
# MAGIC         WHEN 'COP'   THEN 'Energy'
# MAGIC         WHEN 'SPY'   THEN 'ETF'
# MAGIC         WHEN 'QQQ'   THEN 'ETF'
# MAGIC         WHEN 'DIA'   THEN 'ETF'
# MAGIC         WHEN 'IWM'   THEN 'ETF'
# MAGIC         WHEN 'VTI'   THEN 'ETF'
# MAGIC         ELSE 'Unknown'
# MAGIC     END                                   AS Sector
# MAGIC FROM parquet.`/Volumes/stock_pipeline/bronze/stock_data/*/*/*.parquet`
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as null_count
# MAGIC FROM parquet.`/Volumes/stock_pipeline/bronze/stock_data/*/*/*.parquet`
# MAGIC WHERE Close IS NULL 
# MAGIC    OR Ticker IS NULL 
# MAGIC    OR Date IS NULL

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F

# Define sector mapping
sector_map = {
    "META": "FAANG", "AAPL": "FAANG",
    "AMZN": "FAANG", "NFLX": "FAANG",
    "GOOGL": "FAANG", "MSFT": "Big Tech",
    "NVDA": "Big Tech", "AMD": "Big Tech",
    "INTC": "Big Tech", "TSLA": "Big Tech",
    "JPM": "Finance", "GS": "Finance", "BAC": "Finance",
    "MS": "Finance", "V": "Finance", "MA": "Finance",
    "AXP": "Finance", "BLK": "Finance",
    "JNJ": "Healthcare", "PFE": "Healthcare", "UNH": "Healthcare",
    "ABBV": "Healthcare", "MRK": "Healthcare",
    "WMT": "Retail", "TGT": "Retail", "NKE": "Retail",
    "MCD": "Retail", "SBUX": "Retail", "COST": "Retail",
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy",
    "SPY": "ETF", "QQQ": "ETF", "DIA": "ETF",
    "IWM": "ETF", "VTI": "ETF"
}

# Convert dictionary to Spark map
mapping_expr = F.create_map([F.lit(x) for pair in sector_map.items() for x in pair])

print("Sector mapping created")

# COMMAND ----------

silver_df = bronze_df \
    .withColumn("Date", F.col("Date").cast("date")) \
    .withColumn("Sector", mapping_expr[F.col("Ticker")]) \
    .withColumn("Open", F.round(F.col("Open"), 2)) \
    .withColumn("High", F.round(F.col("High"), 2)) \
    .withColumn("Low", F.round(F.col("Low"), 2)) \
    .withColumn("Close", F.round(F.col("Close"), 2)) \
    .withColumn("Volume", F.col("Volume").cast("integer"))

silver_df.show(5)

# COMMAND ----------

# Remove duplicates
silver_df = silver_df.dropDuplicates(["Date", "Ticker"])

# Validate no nulls in critical columns
null_check = silver_df.filter(
    F.col("Close").isNull() | 
    F.col("Ticker").isNull() | 
    F.col("Date").isNull()
).count()

print(f"Rows with nulls in critical columns: {null_check}")
print(f"Total rows after dedup: {silver_df.count()}")

# COMMAND ----------

# Write to silver Delta table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("stock_pipeline.silver.stock_prices")

print("Silver table written successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stock_pipeline.silver.stock_prices
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE stock_pipeline.silver.stock_prices

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Equivalent - Bronze to Silver Transformation
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE stock_pipeline.silver.stock_prices
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CAST(Date AS DATE)                    AS Date,
# MAGIC     Ticker,
# MAGIC     ROUND(Open, 2)                        AS Open,
# MAGIC     ROUND(High, 2)                        AS High,
# MAGIC     ROUND(Low, 2)                         AS Low,
# MAGIC     ROUND(Close, 2)                       AS Close,
# MAGIC     CAST(Volume AS INT)                   AS Volume,
# MAGIC     CASE Ticker
# MAGIC         WHEN 'META'  THEN 'FAANG'
# MAGIC         WHEN 'AAPL'  THEN 'FAANG'
# MAGIC         WHEN 'AMZN'  THEN 'FAANG'
# MAGIC         WHEN 'NFLX'  THEN 'FAANG'
# MAGIC         WHEN 'GOOGL' THEN 'FAANG'
# MAGIC         WHEN 'MSFT'  THEN 'Big Tech'
# MAGIC         WHEN 'NVDA'  THEN 'Big Tech'
# MAGIC         WHEN 'AMD'   THEN 'Big Tech'
# MAGIC         WHEN 'INTC'  THEN 'Big Tech'
# MAGIC         WHEN 'TSLA'  THEN 'Big Tech'
# MAGIC         WHEN 'JPM'   THEN 'Finance'
# MAGIC         WHEN 'GS'    THEN 'Finance'
# MAGIC         WHEN 'BAC'   THEN 'Finance'
# MAGIC         WHEN 'MS'    THEN 'Finance'
# MAGIC         WHEN 'V'     THEN 'Finance'
# MAGIC         WHEN 'MA'    THEN 'Finance'
# MAGIC         WHEN 'AXP'   THEN 'Finance'
# MAGIC         WHEN 'BLK'   THEN 'Finance'
# MAGIC         WHEN 'JNJ'   THEN 'Healthcare'
# MAGIC         WHEN 'PFE'   THEN 'Healthcare'
# MAGIC         WHEN 'UNH'   THEN 'Healthcare'
# MAGIC         WHEN 'ABBV'  THEN 'Healthcare'
# MAGIC         WHEN 'MRK'   THEN 'Healthcare'
# MAGIC         WHEN 'WMT'   THEN 'Retail'
# MAGIC         WHEN 'TGT'   THEN 'Retail'
# MAGIC         WHEN 'NKE'   THEN 'Retail'
# MAGIC         WHEN 'MCD'   THEN 'Retail'
# MAGIC         WHEN 'SBUX'  THEN 'Retail'
# MAGIC         WHEN 'COST'  THEN 'Retail'
# MAGIC         WHEN 'XOM'   THEN 'Energy'
# MAGIC         WHEN 'CVX'   THEN 'Energy'
# MAGIC         WHEN 'COP'   THEN 'Energy'
# MAGIC         WHEN 'SPY'   THEN 'ETF'
# MAGIC         WHEN 'QQQ'   THEN 'ETF'
# MAGIC         WHEN 'DIA'   THEN 'ETF'
# MAGIC         WHEN 'IWM'   THEN 'ETF'
# MAGIC         WHEN 'VTI'   THEN 'ETF'
# MAGIC         ELSE 'Unknown'
# MAGIC     END AS Sector
# MAGIC FROM parquet.`/Volumes/stock_pipeline/bronze/stock_data/*/*/*.parquet`
# MAGIC WHERE Close IS NOT NULL
# MAGIC   AND Ticker IS NOT NULL
# MAGIC   AND Date IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY Date, Ticker ORDER BY Date) = 1
# MAGIC ```