# Databricks notebook source
# MAGIC %md
# MAGIC ### **Incremental Data Ingestion**

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value=dbutils.widgets.get("src")
src_value

# COMMAND ----------

df=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
        .option("cloudFiles.schemaEvolutionMode","rescue")\
        .load(f"/Volumes/workspace/raw_/datavolume/raw_data/{src_value}/")

# COMMAND ----------

df.writeStream.format("delta")\
    .option("checkpointLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/Data")\
    .start()

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql.functions import *
from pyspark.sql.types import *
df=spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/flights/Data/")


# COMMAND ----------

# MAGIC %md
# MAGIC ###**DIPLAY**

# COMMAND ----------

df=spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/flights/Data/")
display(df)

# COMMAND ----------

