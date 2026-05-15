import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="stage_booking"
)
def stage_booking():
    df=spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/Data/")
    return df


@dlt.view(
    name="trans_booking"
)
def trans_booking():
    df=spark.readStream.table("stage_booking")
    df=df.withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("updated_date", current_timestamp()) \
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df


rules={
    "rule1":"booking_id IS NOT NULL",
    "rule2":"passenger_id IS NOT NULL",
    "rule3":"flight_id IS NOT NULL",
    "rule4":"airport_id IS NOT NULL"
}
    

@dlt.table(
    name="silver_booking"
)
@dlt.expect_all_or_drop(rules)
def silver_booking():
    df=dlt.readStream("trans_booking")
    return df

#The abovee code for silver Booking table
##############################################################################################
@dlt.view(
    name="trans_flight"
)
def trans_flight():
    df=spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/Data/")
    df=df.withColumn("flight_date", to_date(col("flight_date")))\
        .withColumn("updated_date", current_timestamp())\
        .drop("_rescued_data")
    return df
dlt.create_streaming_table("sliver_flight")
dlt.create_auto_cdc_flow(
    target="sliver_flight",
    source="trans_flight",
    keys=["flight_id"],
    sequence_by=col("updated_date"),
    stored_as_scd_type=1

)


###############################################################################################
#pssengers
@dlt.view(
    name="trans_passenger"
)
def trans_passenger():
    df=spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/passengers/Data/")
    df=df.withColumn("updated_date", current_timestamp())\
        .drop("_rescued_data")
    return df
dlt.create_streaming_table("silver_passenger")
dlt.create_auto_cdc_flow(
    target="silver_passenger",
    source="trans_passenger",
    keys=["passenger_id"],
    sequence_by=col("updated_date"),
    stored_as_scd_type=1
)
###############################################################################################
#airport
@dlt.view(
    name="trans_airport"
)
def trans_airport():
    df=spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/Data/")
    df=df.drop("_rescued_data")\
        .withColumn("updated_date",current_timestamp())
    return df
dlt.create_streaming_table("silver_airport")
dlt.create_auto_cdc_flow(
    target="silver_airport",
    source="trans_airport",
    keys=["airport_id"],
    sequence_by=col("updated_date"),
    stored_as_scd_type=1
)
##########################################################################################
#silver business view
#@dlt.table(
#    name="silver_business_view"
#)
#def silver_business_view():
 #   df=dlt.readStream("silver_booking")\
  #      .option(ingonoreChanges=True)\
   #     .join(dlt.readStream("sliver_flight"),["flight_id"])\
    #    .join(dlt.readStream("silver_passenger"),["passenger_id"])\
     #   .join(dlt.readStream("silver_airport"),["airport_id"])\
      #  .drop("updated_date")
    #return df



















