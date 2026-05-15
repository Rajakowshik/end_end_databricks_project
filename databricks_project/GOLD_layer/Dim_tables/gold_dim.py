# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("kay_value_list","")

dbutils.widgets.text("cdc_col","")

dbutils.widgets.text("source_object","")

dbutils.widgets.text("source_schema","")

dbutils.widgets.text("target_object","")

dbutils.widgets.text("target_schema","")

# COMMAND ----------

# DBTITLE 1,Cell 3
key_value_col="['passenger_id']"
key_value_list=eval(key_value_col)


backdate_refresh=""


source_object="silver_passenger"

source_schema="silver"

target_object="dim_passenger"

target_schema="gold"

cdc_col="updated_date"

surrogate_key="dimpassengerkey"



# COMMAND ----------

if len(backdate_refresh)==0:

    if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):

        last_load=spark.sql(f"SELECT MAX({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
    else:

        last_load="1900-01-01 00:00:00"
else:
    last_load=backdate_refresh

last_load

# COMMAND ----------

df_src=spark.sql(f"SELECT * FROM workspace.{source_schema}.{source_object} WHERE {cdc_col}>='{last_load}'")
display(df_src)


# COMMAND ----------

key_value_str=', '.join(key_value_list)
key_value_str

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    #it is for incremental load 
    # key value
    key_value_str=', '.join(key_value_list)
    df_trg=spark.sql(f"SELECT {key_value_str},{surrogate_key}, create_date, update_date FROM workspace.{target_schema}.{target_object}")
else:
    # it is for initial load of data
    key_value_init=[f"'' as {i}"for i in key_value_list]
    key_value_init=', '.join(key_value_init)
    df_trg=spark.sql(f"""SELECT {key_value_init},cast('0' as int) as {surrogate_key},cast('1900-01-01 00:00:00' as timestamp) as create_date, cast('1900-01-01 00:00:00'as timestamp) as update_date where 1=0""")


# COMMAND ----------


df_trg.display()

# COMMAND ----------

key_value_str=', '.join(key_value_list)
key_value_str


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 10
join_condition=' AND '.join([f"src.{i}=trg.{i}" for i in key_value_list])
join_condition

# COMMAND ----------

# DBTITLE 1,Cell 11
df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")
df_join=spark.sql(f"""
        SELECT src.*,
        trg.{surrogate_key},
        trg.create_date,
        trg.update_date
        FROM src
        LEFT JOIN trg
        ON {join_condition}         
         """)
display(df_join)


# COMMAND ----------

df_old=df_join.filter(df_join[surrogate_key].isNotNull())
df_new=df_join.filter(df_join[surrogate_key].isNull())

# COMMAND ----------

display(df_old)
display(df_new)

# COMMAND ----------

df_old_enrich=df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    max_surrogate_key=spark.sql(f"""
                            SELECT MAX({surrogate_key}) FROM workspace.{target_schema}.{target_object}
                        """).collect()[0][0]
    df_new=df_new.withColumn(f'{surrogate_key}',lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
        .withColumn('create_date',current_timestamp())\
        .withColumn('update_date',current_timestamp())
else:
    max_surrogate_key=0
    df_new=df_new.withColumn(f'{surrogate_key}',lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
        .withColumn('create_date',current_timestamp())\
        .withColumn('update_date',current_timestamp())

# COMMAND ----------

df_union=df_old_enrich.union(df_new)
df_union.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Cell 19
if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    df_obj=DeltaTable.forName(spark,f"workspace.{target_schema}.{target_object}")
    df_obj.alias("trg").merge(df_union.alias("src"),f"src.{surrogate_key}=trg.{surrogate_key}")\
        .whenMatchedUpdateAll(condition=f"src.{cdc_col}>=trg.{cdc_col}")\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_union.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"workspace.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dim_passenger where passenger_id='P0049'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dim_flight ;

# COMMAND ----------

