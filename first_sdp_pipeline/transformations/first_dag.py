from pyspark import pipelines as dp
from pyspark.sql.functions import *

#Materialize View
@dp.materialized_view(name = "src_sales")       
def src_sales():
    df = spark.read.table("sdp_catalog.source.sales")
    df = df.withColumn("date",to_date(col("date"),"MM-dd-yyyy"))
    return df
#Materialize View (referring to another materialized view)
@dp.materialized_view(name = "enr_sales")      
def enr_sales():
    df = spark.read.table("src_sales")
    df = df.withColumn("revenue",col("revenue")*1.05)
    return df
#Materialized view(referring to another materialized view)


    

