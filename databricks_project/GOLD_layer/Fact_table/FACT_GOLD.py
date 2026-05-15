# Databricks notebook source
# MAGIC %md
# MAGIC ### **Parameters**

# COMMAND ----------

catalog="workspace"

source_schema="silver"

source_object="silver_booking"

cdc_column="updated_date"

backrefresh_date=""

fact_table=f"{catalog}.{source_schema}.{source_object}"

target_schema="gold"

target_object="fact_gold_booking"


fact_dim_keys=["dimpassengerkey","dimflightkey","dimairportkey","booking_date"]


# COMMAND ----------

dimension=[
    {
        "table":f"workspace.{target_schema}.dim_airport",
        "alias":"Dimairport",
        "join_key":[("airport_id","airport_id")]
    },
    {
        "table":f"workspace.{target_schema}.dim_flight",
        "alias":"Dimflight",
        "join_key":[("flight_id","flight_id")]
    },
    {
        "table":f"workspace.{target_schema}.dim_passenger",
        "alias":"Dimpassenger",
        "join_key":[("passenger_id","passenger_id")]
    }
]

fact_dimension=["amount","booking_date","updated_date"]



# COMMAND ----------

if len(backrefresh_date)==0:
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
        last_load=spark.sql(f"SELECT MAX({cdc_column}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]
    else:
        last_load="1900-01-01 00:00:00"
else:
    last_load=backrefresh_date
#test last load
print(last_load)

        

# COMMAND ----------

#df_source=spark.sql(f"SELECT * FROM {catalog}.{target_schema}.{target_object} WHERE {cdc_column}>='{last_load}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ##**SELECT QUERY**

# COMMAND ----------

def get_select_query(fact_dimension,dimension,source_object,source_schema,last_load,catalog,cdc_column):
    fact_alias="f"
    select_fact=[f"{fact_alias}.{col}"for col in fact_dimension]

    join_clues=[]
    for dim in dimension:
        dim_alias=dim["alias"]
        dim_select=f"{dim_alias}.{dim_alias}key"
        select_fact.append(dim_select)


        join_con=[f"{fact_alias}.{fk}={dim_alias}.{dk}"for fk,dk in dim["join_key"]]
        join=f"LEFT JOIN {dim['table']} AS {dim_alias} ON " + " AND ".join(join_con)
        join_clues.append(join)
    select_col=", \n    ".join(select_fact)
    select_join="\n".join(join_clues)
    where_cluse=f"{fact_alias}.{cdc_column} >='{last_load}'"



    query=f"""
    SELECT 
        {select_col} 
    FROM {catalog}.{source_schema}.{source_object} AS {fact_alias}
    {select_join}
    WHERE {where_cluse}
    """

    return query



# COMMAND ----------

query=get_select_query(fact_dimension,dimension,source_object,source_schema,last_load,catalog,cdc_column)



# COMMAND ----------

# MAGIC %md
# MAGIC ##**DF_FACT**

# COMMAND ----------

df_fact=spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ###**UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

fact_dim_key_str=" AND ".join([f"src.{col}=trg.{col}" for col in fact_dim_keys])
print(fact_dim_key_str)

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    df_obj=DeltaTable.forName(spark,f"workspace.{target_schema}.{target_object}")
    df_obj.alias("trg").merge(df_fact.alias("src"),fact_dim_key_str)\
        .whenMatchedUpdateAll(condition=f"src.{cdc_column}>=trg.{cdc_column}")\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"workspace.{target_schema}.{target_object}")

# COMMAND ----------

