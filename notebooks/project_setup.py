# Databricks notebook source
# MAGIC %md
# MAGIC #Setup Project Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS stock_pipeline_ext_location
# MAGIC   URL 'abfss://marketdata@stockpipelineadls.dfs.core.windows.net/'
# MAGIC   WITH (STORAGE CREDENTIAL `stock_pipeline_ext_sc`)
# MAGIC   COMMENT 'External Location for Stock Market Pipeline'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS stock_pipeline
# MAGIC MANAGED LOCATION 'abfss://marketdata@stockpipelineadls.dfs.core.windows.net/'
# MAGIC COMMENT 'Stock Market Pipeline Data Lakehouse';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG stock_pipeline;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC     COMMENT 'Raw ingested stock data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC     COMMENT 'Cleaned and validated stock data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC     COMMENT 'Aggregated business metrics';
# MAGIC
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG stock_pipeline;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS stock_data
# MAGIC     LOCATION 'abfss://marketdata@stockpipelineadls.dfs.core.windows.net/bronze'
# MAGIC     COMMENT 'Bronze layer raw stock data';
# MAGIC
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/stock_pipeline/bronze/stock_data/'
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls("abfss://marketdata@stockpipelineadls.dfs.core.windows.net/")
for f in files:
    print(f.name)

# COMMAND ----------

files = dbutils.fs.ls("abfss://marketdata@stockpipelineadls.dfs.core.windows.net/bronze/stocks/")
for f in files:
    print(f.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VOLUME IF EXISTS stock_pipeline.bronze.stock_data;
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS stock_pipeline.bronze.stock_data
# MAGIC     LOCATION 'abfss://marketdata@stockpipelineadls.dfs.core.windows.net/bronze/stocks'
# MAGIC     COMMENT 'Bronze layer raw stock data';

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/stock_pipeline/bronze/stock_data/'