# Databricks notebook source
src_arr=[
    {"src":"bookings"},
    {"src":"flights"},
    {"src":"airports"},
    {"src":"passengers"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "outvalue", value = src_arr)



# COMMAND ----------

