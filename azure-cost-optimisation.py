# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer

# COMMAND ----------

bronze_table_name = "bronze_cost_exports"
silver_table_name = "silver_cost_exports"
bronze_checkpoint_loc = "abfss://bronze@azurecostoptimisation.dfs.core.windows.net/checkpoint"
silver_checkpoint_location = "abfss://silver@azurecostoptimisation.dfs.core.windows.net/checkpoint"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.azurecostoptimisation.dfs.core.windows.net", dbutils.secrets.get(scope="costoptimisationScope",key="costoptimisationAK"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_first_last_date_of_month():
    todays_date= datetime.today()
    first_day = todays_date + relativedelta(day=1)
    last_day = todays_date + relativedelta(day=31)
    
    first_date = first_day.date()
    last_date = last_day.date()
    
    return first_date.strftime('%Y%m%d') + '-'+ last_date.strftime('%Y%m%d')

# COMMAND ----------

from pyspark.sql.functions import lower, input_file_name

current_month_dir = get_first_last_date_of_month()
checkpoint_directory = "cost/"

bucket_location = "abfss://test-daily-exports@azurecostoptimisation.dfs.core.windows.net/" + checkpoint_directory

raw_df = (spark.readStream
          .format("cloudFiles")
          .option("header", "true")
          .option("delimiter", ",")
          .option("escape",'"')
          .option("multiline", "true")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", bronze_checkpoint_loc)
          .option("recursiveFileLookup","true")
          .load(bucket_location)
          .select("*", current_timestamp().alias("processing_time"))
    )


raw_df = (raw_df
             .withColumn('tags', lower('tags'))
             .withColumn('ResourceId', lower('ResourceId'))
             .withColumn('resourceGroupName', lower('resourceGroupName'))
             .withColumn("filePath",input_file_name())
         )

raw_df.writeStream\
          .outputMode("append")\
          .option("checkpointLocation", bronze_checkpoint_loc)\
          .option("mergeSchema", "true")\
          .trigger(availableNow=True)\
          .table(bronze_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW view_unique_bronze_cost_exports AS (
# MAGIC   SELECT *,
# MAGIC   tags:client AS tags_client,
# MAGIC   tags:clusterid AS tags_clusterid,
# MAGIC   tags:clustername AS tags_clustername,
# MAGIC   date_format(coalesce(to_timestamp(tags:created), to_timestamp(tags:created, "MM/dd/yyyy HH:mm:ss")),'yyyy-MM-dd HH:mm:ss') AS tags_created,
# MAGIC   tags:creator AS tags_creator,
# MAGIC   tags:owner AS tags_owner,
# MAGIC   tags:purpose AS tags_purpose,
# MAGIC   tags:resourceclass AS tags_resourceclass,
# MAGIC   tags:vendor AS tags_vendor
# MAGIC   FROM bronze_cost_exports
# MAGIC   WHERE tags IS NOT NULL AND processing_time = (
# MAGIC     SELECT MAX(processing_time)
# MAGIC     FROM bronze_cost_exports
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW agg_bronze_cost_exports AS (
# MAGIC   SELECT Date, ResourceId, MeterId, tags_owner, resourceGroupName, SUM(CostInBillingCurrency) AS CostInBillingCurrency 
# MAGIC   FROM view_unique_bronze_cost_exports
# MAGIC   WHERE tags_owner IS NOT NULL
# MAGIC   GROUP BY Date, ResourceId, MeterId, tags_owner, resourceGroupName
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_cost_exports AS (
# MAGIC   SELECT * FROM agg_bronze_cost_exports WHERE 1=2
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_cost_exports T
# MAGIC USING agg_bronze_cost_exports S
# MAGIC ON T.Date = S.Date AND T.ResourceId = S.ResourceId AND T.MeterId = S.MeterId
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

import pandas
def store_aggregation_to_csv(gold_tbl_name):
    file_name = gold_tbl_name+".csv"
    storage_path = "abfss://gold@azurecostoptimisation.dfs.core.windows.net/"
    df_result = spark.sql("SELECT * FROM {gold_tbl_name}".format(gold_tbl_name=gold_tbl_name))
   
    df_result.toPandas().to_csv("/dbfs/FileStore/" + file_name, index=False)
    dbutils.fs.mv('/FileStore/'+ file_name, storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Daily Resource Consumption

# COMMAND ----------

## Using silver table as a gold table for 1st level of aggregation i.e. getting unique resource details
gold_tbl_name = 'silver_cost_exports'
store_aggregation_to_csv(gold_tbl_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table last_7_day_top_35_cost as (
# MAGIC Select ResourceId, MeterId, tags_owner, resourceGroupName , sum(CostInBillingCurrency) as total_cost from (
# MAGIC  select *
# MAGIC  from silver_cost_exports
# MAGIC   where to_timestamp(date, 'MM/dd/yyyy') > dateadd(day,-7,getdate()))
# MAGIC  group by ResourceId, MeterId, tags_owner, resourceGroupName
# MAGIC  order by sum(CostInBillingCurrency) desc
# MAGIC  limit 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table last_14_day_top_50_cost as (
# MAGIC Select ResourceId, MeterId, tags_owner, resourceGroupName , sum(CostInBillingCurrency) as total_cost from (
# MAGIC  select *
# MAGIC  from silver_cost_exports
# MAGIC   where to_timestamp(date, 'MM/dd/yyyy') > dateadd(day,-14,getdate()))
# MAGIC  group by ResourceId, MeterId, tags_owner, resourceGroupName
# MAGIC  order by sum(CostInBillingCurrency) desc
# MAGIC  limit 50)

# COMMAND ----------

gold_tbl_name = 'last_7_day_top_35_cost'
store_aggregation_to_csv(gold_tbl_name)

# COMMAND ----------

gold_tbl_name = 'last_14_day_top_50_cost'
store_aggregation_to_csv(gold_tbl_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table last_30_day_top_250_cost as (
# MAGIC Select ResourceId, MeterId, tags_owner, resourceGroupName , sum(CostInBillingCurrency) as total_cost from (
# MAGIC  select *
# MAGIC  from silver_cost_exports
# MAGIC   where to_timestamp(date, 'MM/dd/yyyy') > dateadd(day,-30,getdate()))
# MAGIC  group by ResourceId, MeterId, tags_owner, resourceGroupName
# MAGIC  order by sum(CostInBillingCurrency) desc
# MAGIC  limit 250)

# COMMAND ----------

gold_tbl_name = 'last_30_day_top_250_cost'
store_aggregation_to_csv(gold_tbl_name)
